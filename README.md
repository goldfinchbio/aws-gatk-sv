## Note
  This repo is no longer maintained by Goldfinch Bio. Please use https://github.com/LokaHQ/aws-gatk-sv instead.

# AWS Setup & Execution
  This document provides all the relevant steps needed for execution of this pipeline on AWS Infrastructure.
  For this the pre-requisites are as below:
  - Cromwell
  - S3
  - FSx (optional)
  - AWS Batch Compute
  - CloudWatch

  All the above will be taken care of by following the guidelines/suggestions as per the blog and [Genomics Workflows on AWS](https://github.com/aws-samples/aws-genomics-workflows) 

  Once the above is setup, you can now move onto setting up the gatk-sv codebase for execution.
  
  Important : MELT framework of the pipeline is optional and depends upon the licensing. 
  If the license is available, please create a melt docker manually as per steps in docs/create_melt_docker.md
  
  If the license is not available, please mark "use_melt" flags as "false". This can also be achieved by uncommenting below lines from the setup script before running the setup.
  https://github.com/goldfinchbio/aws-gatk-sv/blob/master/scripts/aws_setup_script.sh#L49
  https://github.com/goldfinchbio/aws-gatk-sv/blob/master/scripts/aws_setup_script.sh#L50

  Note this would need manual invocation of setup script from EC2 instance.

## Steps
  We now deploy the codebase and create required json files for the pipeline using SSM Document.

  Deploy the SSM Document using following command using the profile of your desired AWS Account & Region
  ```bash
        wget https://github.com/goldfinchbio/aws-gatk-sv/blob/master/templates/cf_ssm_document_setup.yaml\?raw\=true -O cf_ssm_document_setup.yaml
        aws cloudformation deploy --profile <AWS_PROFILE> --region <AWS_REGION> --stack-name "gatk-sv-ssm-deploy" --template cf_ssm_document_setup.yaml
  ```

  Now Login to the console.
  AWS Systems Manager -> Documents -> Owned By Me -> Search for "gatk-sv-ssm-deploy" -> Execute Automation -> Select the EC2 for cromwell-server and Fill in S3OrFSXPath with correct FSX or S3 Path -> Execute.

  This will do the following :
  - Clone the Repo : 
  - Create the json file input for the pipeline.
  - Replace it with AWS Paths/ECR Images
  - Update MELT Flag to false and also change cpu/mem for TinyResolve job.
  - Download the Docker/GCR Images and upload those to ECR.

  If there are any changes needed, run the code manually for setup as below :

    - Download the script gatk-sv/scripts/aws/aws_setup_script.sh on the home path of the cromwell EC2 instance and update the variables inside it. The variable list is as below :
    ```bash
        BROAD_REF_PATH="S3 or FSx mount path where broad-ref reference files exist"
        CRAM_BAM_FILE_PATH="S3 or FSx mount path where Sample BAM or CRAM (wth index) files exist"
        HAPLOTYPE_GVCF_PATH="S3 or FSx mount path where Haplotype caller gvcf files exist"
        GATK_SV_RESOURCES_PATH="S3 or FSx mount path where gatk-sv-resources files exist"
        BATCH_DEF_PATH="S3 or FSx mount path where batch_sv.test_large.qc_definitions.tsv or equivalent file resides"
        AWS_ACCOUNT_ID="AWS Account Id"
        AWS_REGION="AWS Region in which operating"
        ECR_REPO_NAME="ECR Repo Name needed ex: 'sv-pipeline'"
    ```

    - Run the script. It will setup the codebase and also upload the required images to ECR.
    ```bash
        sh aws_setup_script.sh
    ```

- Final and manual step. Compare the 2 files created on EC2 Instance.
    - BROAD : gatk-sv/gatk_run/GATKSVPipelineBatch.json
    - AWS : gatk-sv/gatk_run/aws_GATKSVPipelineBatch.json

    And update the missing params in AWS json from BROAD json with correct AWS paths/account id/aws region and at the same location as of BROADs.

- Run the pipeline from EC2 home path
```bash
    cromshell submit /home/ec2-user/gatk-sv/gatk_run/wdl/GATKSVPipelineBatch.wdl /home/ec2-user/gatk-sv/gatk_run/aws_GATKSVPipelineBatch.json /home/ec2-user/gatk-sv/gatk_run/opts.json /home/ec2-user/gatk-sv/gatk_run/wdl/dep.zip
```

- Monitoring the pipeline either from AWS Batch Dashboard or via ommand line of cromwell EC2 by running below command :
```bash
    cromshell status
```

The mapping of AWS Batch Job Names and GATK-SV Module and Sub-module name can be viewed from the [Job_Names_and_Modules.csv](https://github.com/goldfinchbio/aws-gatk-sv/blob/master/configs/job_names_and_modules.csv)
