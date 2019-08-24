import json
import logging
import boto3
import time

s3 = boto3.resource('s3')
s3_con=boto3.client('s3')
gl = boto3.client('glue')
#/Quantum-source-file
def lambda_handler(event, context):
    #bucket = s3.Bucket('smart-ingest-bucket')
    #for obj in bucket.objects.all():
    #    file_contents = obj.get()["Body"].read()
        #print (file_contents.count)
    #    print(obj)
    
    # get the file object
    #obj = s3.Object('smart-ingest-bucket', 'Quantum-source-file/')
    # read the file contents in memory
    #file_contents = obj.get()["Body"].read()
    # print the occurrences of the new line character to get the number of lines
    #print (file_contents)
    cnt = 0
    bucket='smart-ingest-bucket'
    File='Quantum-source-file/'
    objs = boto3.client('s3').list_objects_v2(Bucket=bucket,Prefix=File)
    fileCount = objs['KeyCount']
    for object in objs['Contents']:
        if object['Size'] == 0:
            # Print name of zero-size object
            cnt = cnt+1
            print('null count',cnt)
    final_result = fileCount-cnt
    if final_result == 2:
        gl.start_job_run(JobName = 'populate-quantum-stg')
        print('Triggering Glue job')
    else:
        time.sleep(1)