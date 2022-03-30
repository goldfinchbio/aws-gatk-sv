"""
This script will be used to get the batch status and also to gather container resource usage if specified.

Examples:

    python3 get_batch_status.py -j "default-gwf-core-dev-sv-fsx-SCRATCH" -s "2022-03-23 11:47:00"
    python3 get_batch_status.py -j "default-gwf-core-dev-sv-fsx-SCRATCH" -s "2022-03-23 11:47:00" -l "/aws/ecs/containerinsights/spot-gwf-core-dev-sv-fsx-SCRATCH_Batch_64d2263e-1ba8-3c96-a34f-2d6ab90f1ebc/performance"
    python3 get_batch_status.py -j "default-gwf-core-dev-sv-fsx-SCRATCH" -s "2022-03-24 20:00:00" -l "/aws/ecs/containerinsights/spot-gwf-core-dev-sv-fsx-SCRATCH_Batch_64d2263e-1ba8-3c96-a34f-2d6ab90f1ebc/performance"
    python3 get_batch_status.py -j "default-gwf-core-dev-sv-fsx-SCRATCH" -s "2022-03-24 20:00:00" -l "/aws/ecs/containerinsights/spot-gwf-core-dev-sv-fsx-SCRATCH_Batch_64d2263e-1ba8-3c96-a34f-2d6ab90f1ebc/performance" -i 10000

"""

import boto3
import pandas as pd
import datetime, time
import numpy as np
import os
import argparse
import logging
import ast


# Initialise Logger
format_string = "%(asctime)s %(name)s [%(levelname)s] %(message)s"
logger = logging.getLogger("batch-status-report")
handler = logging.StreamHandler()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(format_string)
handler.setFormatter(formatter)
logger.addHandler(handler)


def parse_args():
    """
    Use the argparse library to provide special help text and simplify argument handling

    :return: tuple
    """
    parser = argparse.ArgumentParser(description='Run Batch Status. This can take multiple arguments.')
    parser.add_argument('-p', '--aws_profile', dest='aws_profile', required=False, default="default",
                        help="The AWS Profile to be used.")
    parser.add_argument('-r', '--aws_region', dest='aws_region', required=False, default="us-east-2",
                        help="The AWS Region to be used.")
    parser.add_argument('-j', '--batch_job_queue', dest='batch_job_queue', required=True,
                        help="The AWS Batch Job Queue which needs to be looked at for jobs.")
    parser.add_argument('-s', '--batch_start_time', dest='batch_start_time', required=True, 
                        help="Provide the Batch Start Time after which the jobs are needed to be looked upon. Should be in YYYY-MM-DD HH:MM:SS format only")
    parser.add_argument('-l', '--cloudwatch_log_group_name', dest='cloudwatch_log_group_name', required=False, default=None,
                        help="This is needed only when the cloudwatch container insights is enabled as per : https://docs.opendata.aws/genomics-workflows/orchestration/cost-effective-workflows/cost-effective-workflows.html .")
    parser.add_argument('-i', '--cloudwatch_query_interval', dest='cloudwatch_query_interval', required=False, default=600, type=int,
                        help="The interval is the duration (secs) for which the query needs to be executed. This shouldn't be more coz the query only returns 10k records and Cloudwatch saves log for each job per minute. Calculate accordingly. ")
    
    args = parser.parse_args()
    return args.aws_profile, args.aws_region, args.batch_job_queue, args.batch_start_time, args.cloudwatch_log_group_name, args.cloudwatch_query_interval


def get_batch_job_id_list():
    """This function will just get the job ids as per status

    Returns:
        List: List of all Job Ids from these 3 statuses : 'SUCCEEDED', 'FAILED', 'RUNNING'
    """
    status_needed = ['SUCCEEDED', 'FAILED', 'RUNNING']
    logger.info("Running for following statuses only : %s " % str(status_needed))
    epoch_batch_start = time.mktime(datetime.datetime.fromisoformat(batch_start_time).timetuple()) * 1000
    logger.info("Epoch Batch Start Time : %s" % epoch_batch_start)
    job_id_list = []

    for job_status in status_needed:
        temp_job_ids = []
        logger.info("Getting Jobs IDs with %s status" % job_status)
        response = batch_client.list_jobs(
            jobQueue=batch_job_queue,
            jobStatus=job_status,
            maxResults=123
        )

        for x in response['jobSummaryList']:
            if x['createdAt'] > epoch_batch_start:
                temp_job_ids.append(x['jobId'])

        nextToken = response['nextToken'] if 'nextToken' in str(response) else None

        # Paginate in loop
        while nextToken != None:
            response = batch_client.list_jobs(
                jobQueue=batch_job_queue,
                jobStatus=job_status,
                maxResults=123,
                nextToken=nextToken
            )
            
            for x in response['jobSummaryList']:
                if x['createdAt'] > epoch_batch_start:
                    temp_job_ids.append(x['jobId'])
            
            if 'nextToken' in response.keys():
                nextToken = response['nextToken']
            else:
                nextToken = None
            
        logger.info("No. of jobs with %s status : %s" % (job_status, len(temp_job_ids)))
        job_id_list.extend(temp_job_ids)

    logger.info("No. of Job Ids fetched : %s " % len(job_id_list))
    return job_id_list


def get_job_details(job_id_list):
    """This function will return all the detail related to a job

    Args:
        job_id_list (List): The list of job ids for which the details are needed to be fetched.

    Returns:
        Pandas DF: The Pandas DF with all the job details.
    """
    final_list = []
    for job_id in job_id_list:
        try:
            response = batch_client.describe_jobs(jobs=[job_id])
            job_status = response['jobs'][0]['status']
            if job_status == "RUNNING":
                container_instance_arns = [response['jobs'][0]['container']['containerInstanceArn'].split("/")[-1]]
                task_ids = [response['jobs'][0]['container']['taskArn'].split("/")[-1]]
            else:
                container_instance_arns = list(set([i['container']['containerInstanceArn'].split("/")[-1] for i in response['jobs'][0]['attempts']]))
                task_ids = list(set([i['container']['taskArn'].split("/")[-1] for i in response['jobs'][0]['attempts']]))

            reasons = None
            for i in response['jobs'][0]['attempts']:
                if 'reason' in str(i):
                    reasons = i['container']['reason'] if not reasons else reasons + \
                        "|" + i['container']['reason']

            final_list.append({
                'job_id': job_id,
                'job_name': response['jobs'][0]['jobName'],
                'job_status': job_status,
                'started_at': response['jobs'][0]['startedAt'] if 'startedAt' in response['jobs'][0] else None,
                'stopped_at': response['jobs'][0]['stoppedAt'] if 'stoppedAt' in response['jobs'][0] else None,
                "image": response['jobs'][0]['container']['image'],
                "vcpus":  response['jobs'][0]["container"]['vcpus'] if 'vcpus' in str(response['jobs'][0]["container"]) else response['jobs'][0]["container"]['resourceRequirements'][0]['value'],
                "memory":  response['jobs'][0]["container"]['memory'] if 'memory' in str(response['jobs'][0]["container"]) else response['jobs'][0]["container"]['resourceRequirements'][1]['value'],
                'num_attempts': len(response['jobs'][0]['attempts']),
                'num_of_instances': len(container_instance_arns),
                'instance_arn_endings': '|'.join(container_instance_arns),
                'task_id': '|'.join(task_ids),
                "reasons": reasons
            })
        except:
            logger.warning("The job id : %s failed parsing with response: %s" % (job_id, response))

    df = pd.DataFrame(final_list)
    df['started_at'] = pd.to_datetime(df['started_at'], errors="coerce", unit='ms')
    df['stopped_at'] = pd.to_datetime(df['stopped_at'], errors="coerce", unit='ms')
    return df


def get_resource_usage():
    """This function will fetch the cpu and mem usage from cloudwatch insights -> container Insights

    Returns:
        Pandas DF: The dataframe with the task and resource consumption details.
    """
    query = "fields @message"  
    epoch_start = int(datetime.datetime.strptime(batch_start_time, "%Y-%m-%d %H:%M:%S").timestamp())
    epoch_end = epoch_start + cloudwatch_query_interval
    current_epoch_time = int(datetime.datetime.now().timestamp())
    final_output = []

    logger.info("Starting to query Cloud Watch Logs Group with an Interval : %s from start time : %s" % (cloudwatch_query_interval, batch_start_time))
    logger.info("Please be patient, this may take sometime...")
    try:
        while epoch_end < current_epoch_time:
            # logger.info("Query Input --- Start Time : %s --- End Time : %s" % (epoch_start, epoch_end))
            start_query_response = log_client.start_query(
                logGroupName=cloudwatch_log_group_name,
                startTime=epoch_start,
                endTime=epoch_end,
                queryString=query,
                limit=10000
            )

            query_id = start_query_response['queryId']
            logger.info("The query id being executed is : %s" % query_id)

            response = None
            while response == None or response['status'] == 'Running':
                # logger.info('Waiting for query to complete ...')
                time.sleep(1)
                response = log_client.get_query_results(queryId=query_id)

            # logger.info("The number of records from query are : %s" % len(response['results']))

            for record in response['results']:
                temp_value =  ast.literal_eval(record[0]['value'])
                if temp_value['Type'] == 'Task':
                    final_output.append(
                        {
                            'task_id': temp_value['TaskId'],
                            'container_instance_id': temp_value['ContainerInstanceId'],
                            'cpu_used': temp_value['CpuUtilized'],
                            'cpu_passed': temp_value['CpuReserved'],
                            'memory_used': temp_value['MemoryUtilized'],
                            'memory_passed': temp_value['MemoryReserved']
                        }
                    )

            epoch_start = epoch_end
            epoch_end = epoch_start + cloudwatch_query_interval
    except:
        logger.error("The cloudwatch log parsing has failed. Kindly check.", exc_info=True)

    final_df = pd.DataFrame(final_output)

    if len(final_df) > 0:
        final_df = final_df.groupby(['task_id', 'container_instance_id']).mean().reset_index()
        final_df.to_csv("%s/task_resource_usage.csv" % output_path, index=False)
        
    logger.info("The number of tasks for which resource usage was gathered is as : %s" % len(final_df))

    return final_df


def get_summaries(job_details_df):
    """This function will gather the different levels of summaries

    Args:
        job_details_df (Pandas DF): The DF with all job details.
    """
    job_modules_df = pd.read_csv("%s/../configs/job_names_and_modules.csv" % current_script_dir)

    # Sub Module Level Summary
    logger.info("Considering only Successful Jobs")
    success_job_details_df = job_details_df[job_details_df['job_status'].isin(["SUCCEEDED"])]
    temp_df = success_job_details_df[['job_name', 'started_at', 'stopped_at']]
    temp_df['avg_duration_across_all_jobs'] = pd.to_datetime(temp_df['stopped_at'], infer_datetime_format=True)  - pd.to_datetime(temp_df['started_at'], infer_datetime_format=True)
    temp_df['avg_duration_across_all_jobs'] = temp_df['avg_duration_across_all_jobs'] / np.timedelta64(1, 'h')
    temp_df = pd.merge(temp_df, job_modules_df, on="job_name", how="left")
    submodule_summary_df = temp_df.groupby(['module_name', 'module_number', 'main_module_name']).agg({'started_at': np.min, 'stopped_at': np.max, 'avg_duration_across_all_jobs': np.average}).reset_index()
    submodule_summary_df = pd.merge(submodule_summary_df, temp_df.value_counts(['main_module_name', 'module_name', 'module_number']).reset_index(name='job_counts'), on="module_name", how="left")
    submodule_summary_df['duration'] = pd.to_datetime(submodule_summary_df['stopped_at'], infer_datetime_format=True)  - pd.to_datetime(submodule_summary_df['started_at'], infer_datetime_format=True)
    submodule_summary_df['duration'] = (submodule_summary_df['duration'] / np.timedelta64(1, 'h'))
    submodule_summary_df = submodule_summary_df[['module_number_x', 'main_module_name_x', 'module_name', 'started_at', 'stopped_at', 'avg_duration_across_all_jobs', 'job_counts', 'duration']].sort_values(["module_number_x", "started_at"])
    submodule_summary_df.to_csv("%s/submodule_summary.csv" % output_path, index=False)
    logger.info("The submodule level summary was written to output directory.")

    # Get High level Summary
    highlevel_summary_df = submodule_summary_df.groupby(["module_number_x", 'main_module_name_x']).agg({'started_at': 'min', 'stopped_at': 'max', 'job_counts': 'sum'}).reset_index()
    highlevel_summary_df['duration'] = (pd.to_datetime(highlevel_summary_df['stopped_at']) - pd.to_datetime(highlevel_summary_df['started_at'])).dt.total_seconds()
    highlevel_summary_df['duration'] = pd.to_datetime(highlevel_summary_df["duration"], unit='s').dt.strftime("%H:%M:%S")
    highlevel_summary_df.to_csv("%s/highlevel_summary.csv" % output_path, index=False)
    logger.info("The highlevel level summary was written to output directory.")


def main():
    """
    This is the main function calling all the sub-sections of the batch status report.
    """
    job_id_list = get_batch_job_id_list()
    if job_id_list:
        batch_job_details_df = get_job_details(job_id_list)
        print(batch_job_details_df)
        if len(batch_job_details_df) > 0 and cloudwatch_log_group_name:
            container_resource_consumption_df = get_resource_usage()
            print(container_resource_consumption_df)
            if len(container_resource_consumption_df) > 0:
                # Join the DFs
                output_df = pd.merge(batch_job_details_df, container_resource_consumption_df, on="task_id", how="left")
                print(output_df)
        else:
            output_df = batch_job_details_df

        output_df.to_csv("%s/%s_job_details.csv" % (output_path, batch_job_queue.replace("-","_")), index=False)

        # Gather Summaries
        get_summaries(output_df)

        # output few important stats
        logger.info("========================================================================================================================================================")
        logger.info("The current status :")
        logger.info("Total number of jobs till now : %s" % len(output_df))
        logger.info(output_df.groupby('job_status').count()['job_id'])
        logger.info("========================================================================================================================================================")
        # logger.info(df.groupby('job_name').count()['job_id'])
        # logger.info("The jobs with multiple attempts.")
        # logger.info(df[df['num_attempts'] > 1][['job_id', 'job_name', 'job_status', 'num_attempts', 'reasons']])
        # logger.info("========================================================================================================================================================")
        instance_arn_list = list(output_df['instance_arn_endings'].unique())
        instances_list = []

        for i in instance_arn_list:
            instances_list.extend(i.split("|"))

        instances_list = list(set(instances_list))
        logger.info("The no. of ec2 instances used are : %s" % len(instances_list))
        logger.info("========================================================================================================================================================")
        logger.info("\n")


if __name__ == '__main__':
    # Reading passed arguments
    aws_profile, aws_region, batch_job_queue, batch_start_time, cloudwatch_log_group_name, cloudwatch_query_interval = parse_args()
    logger.info("AWS Profile : %s" % aws_profile)
    logger.info("AWS Region : %s" % aws_region)
    logger.info("Batch Job Queue : %s" % batch_job_queue)
    logger.info("Batch Start Time : %s" % batch_start_time)
    logger.info("Cloud Watch Log Group Name : %s" % cloudwatch_log_group_name)
    logger.info("Cloud Watch Query Interval Seconds : %s" % cloudwatch_query_interval)

    # Setting up Boto3 Batch and Log Clients
    if aws_profile != "default":
            session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
    else:
            session = boto3.Session(region_name=aws_region)

    batch_client = session.client('batch')
    log_client = session.client('logs')

    # Setting up directories
    current_script_dir = os.path.dirname(os.path.realpath(__file__))
    output_path = "%s/output/%s" % (current_script_dir, "{:%Y_%m_%d_%H_%M_%S}".format(datetime.datetime.now()))
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    logger.info("Current Script Dir : %s" % current_script_dir)
    logger.info("Output Dir: %s" % output_path)

    logger.info("Calling main function")
    main()
    logger.info("The script will end now.")

