"""
This script is for checking if the results of the pipeline allign with the ones from Broad (GCP) Run.
It will need the following :
- EMR Cluster with Hail installed. Needs pyspark for Hail.
- Input vcf.gz files. This can be found from below directories:
    - For GCP : Refer : https://github.com/broadinstitute/gatk-sv/blob/master/inputs/values/ref_panel_1kg.json 
        Key is "cleaned_vcf"
        "clean_vcf": "gs://gatk-sv-ref-panel-1kg/outputs/GATKSVPipelineBatch/38c65ca4-2a07-4805-86b6-214696075fef/call-MakeCohortVcf/MakeCohortVcf/bfd4ef15-d9ea-437b-ade4-bc08e79c29a8/call-CleanVcf/CleanVcf/cda267a8-81b0-46af-aab7-938f8cd42c4f/call-ConcatCleanedVcfs/cacheCopy/ref_panel_1kg.cleaned.vcf.gz",
    - For AWS : You can find it from the latest run, either on s3 or FSx wherever you ran it at below similar path :
        S3: s3://S3RESULTS_BUCKET/cromwell-execution/GATKSVPipelineBatch/<cromwell-workflow-id>/call-MakeCohortVcf/MakeCohortVcf/<random-id-generated-during-execution>/call-CleanVcf/CleanVcf/<random-id-generated-during-execution>/call-ConcatCleanedVcfs/cacheCopy/ref_panel_1kg.cleaned.vcf.gz"
        FSxL: /S3RESULTS_BUCKET/cromwell-execution/GATKSVPipelineBatch/<cromwell-workflow-id>/call-MakeCohortVcf/MakeCohortVcf/<random-id-generated-during-execution>/call-CleanVcf/CleanVcf/<random-id-generated-during-execution>/call-ConcatCleanedVcfs/cacheCopy/ref_panel_1kg.cleaned.vcf.gz"

- Copy the files in an S3 bucket which is accessible by EMR cluster.
- Copy this script on EMR.
- Update the "file_mapping" dictionary accordingly.
- Execute the script.

"""

# Following Steps are needed if executed by a Sagemaker Notebook which can be connected to EMR
# Ensure necessary permissions are present.
# Connect Livy on EMR
# %%bash
# ~/SageMaker/bin/list-clusters

# %reload_ext sparkmagic.magics
# %spark add -s <user_name> -l python -u http://<LIVY_IP_ADDRESS>:8998 -t None

# Import required packages
from pyspark import SparkContext
import hail as hl
import hail.expr.aggregators as agg
hl.init(sc, default_reference='GRCh38')
# import other packages
import numpy as np
import pandas as pd
from math import log, isnan
from pprint import pprint
from collections import Counter
from bokeh.plotting import figure, output_file, save


# Go in loop for each type of run and identify and print the counts
file_mapping = {
    # Always start with GCP as that will become th ref table for precision and comparison
    'gcp_broad': 's3://S3_BUCKET_NAME/structural-variants/concordance_analysis/vcfs/gcp_broad_latest/ref_panel_1kg_v1.cleaned.vcf.gz',
    'aws_fsx_with_melt': 's3://S3_BUCKET_NAME/structural-variants/concordance_analysis/vcfs/fsx_with_melt/ref_panel_1kg.cleaned.vcf.gz',
    'aws_fsx_without_melt': 's3://S3_BUCKET_NAME/structural-variants/concordance_analysis/vcfs/fsx_without_melt/ref_panel_1kg.cleaned.vcf.gz',
    'aws_s3_without_melt': 's3://S3_BUCKET_NAME/structural-variants/concordance_analysis/vcfs/s3/ref_panel_1kg_v2.cleaned.vcf.gz'
}

final_list = []

def get_recall_and_precision_details(ref_mt, input_mt):
    """
    This function will be used to get the recall/sensitivity and precision details
    Recall/Sensitivity = TP/P
    Precision = TP / (TP + FP)
    """
    # Join by locus
    mt_filt = input_mt.semi_join_rows(ref_mt.rows())
    mt_filt.count()

    # Calculate recall/sensitivity
    print('recall/sensitivity')
    print(mt_filt.count()[0]/ref_mt.count()[0])

    # Calculate precision
    print ('precision')
    print(mt_filt.count()[0]/input_mt.count()[0])

    
for file_type,file_location in file_mapping.items():
    print("Running for : %s" % file_type)

    # Import
    mt = hl.import_vcf(file_location, reference_genome='GRCh38', array_elements_required=False, skip_invalid_loci=True, force_bgz=True)
    mt = mt.persist()
    mt = hl.variant_qc(mt, name='var_qc_current_cohort')

    # Get counts
    print(mt.count())
    
    # Get Aggregated rows
    agg_dict = mt.aggregate_rows(hl.agg.counter(mt.info.SVTYPE))
    print(agg_dict)
    agg_dict['run_type'] = file_type
    final_list.append(agg_dict)
    
    # Get Aggregate on Framework Level
    print(mt.aggregate_rows(hl.agg.counter(mt.info.ALGORITHMS[0])))
    print("\n")
    
    if file_type == "gcp_broad":
        gcp_mt = mt
    elif file_type == "aws_fsx_with_melt":
        get_recall_and_precision_details(gcp_mt, mt)
    elif file_type == "aws_fsx_without_melt":
        get_recall_and_precision_details(gcp_mt, mt)
    elif file_type == "aws_s3_without_melt":
        get_recall_and_precision_details(gcp_mt, mt)
    

final_df = pd.DataFrame(final_list)
print(final_df)