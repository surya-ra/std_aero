import json
import logging
import boto3

s3_con=boto3.client('s3')
gl = boto3.client('glue')

def lambda_handler(event, context):
    # TODO implement
    gl.start_job_run(JobName = 'populate-rpa-stg')