import json
import boto3

s3_con=boto3.client('s3')
gl = boto3.client('glue')

def lambda_handler(event, context):
    job_name_store = event['detail']['jobName']
    job_state = event['detail']['state']
    if job_name_store == 'populate-navixa-stg' and job_state == 'SUCCEEDED':
        print('job triggered')
        gl.start_job_run(JobName = 'populate-final-rds')
    else:
        print('job not triggered')