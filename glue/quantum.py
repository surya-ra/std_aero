# Project Title : STD AERO POC- populate Quantum staging bucket
# __authors__ = Nagasree
# __contact__ = nagasree.nagabhushana-rao-gari@capgemini.com
# __maintainer__ = "developer" 
#_Last_Modified_By = “Nagasree”
#_Last_Modified_Date= 5/14/2019  

from pyspark.sql.types import IntegerType, StringType, StructType, ArrayType, StructField
from pyspark.sql.functions import *
from pyspark.sql.functions import split,trim, greatest
import sys
from pyspark.sql import Row
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import explode 
from pyspark.sql.functions import lit
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
import pyspark.sql.functions as f
from functools import reduce
from pyspark.sql.types import *
import datetime
import boto3
import json

#initiate glue context
glueContext = GlueContext(SparkContext.getOrCreate())

##Loading the source files from s3 bucket and converting the corresponding dynamic dataframes to apache spark data frames
Jas_Labour_Paid_dyf=glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options = {"paths":["s3://smart-ingest-bucket/Quantum-source-file/jas_labor_paid_export.csv"]}, format="csv",format_options={'withHeader' : True})
Jas_Labour_Paid_df = Jas_Labour_Paid_dyf.toDF()

Jas_Labour_Production_dyf=glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options = {"paths":["s3://smart-ingest-bucket/Quantum-source-file/jas_labor_production_export.csv"]}, format="csv",format_options={'withHeader' : True})
Jas_Labour_Production_df = Jas_Labour_Production_dyf.toDF()

##Performing Transformations on Jas_Labour_Paid_df
##Jas_Labour_Paid_df.printSchema()
Jas_Labour_Paid_df=Jas_Labour_Paid_df.select(col('EXTERNAL_ID').alias('EMPLOYEE_NUMBER'),\
col('USER_NAME').alias('EMPLOYEE_NAME'),'ATTENTION',\
trim(split(col('DEPT_NAME'),'-')[0]).alias('PROGRAM_DESC'),\
to_date(substring(col('TIME_START'),1,9),'dd-MMM-yy').alias('TRANSACTION_DATE'),\
col('HOURS_OVER_TIME').cast(DoubleType()).alias('AVAIL_OT_HRS'),\
col('HOURS_TOTAL').cast(DoubleType()).alias('HOURS_TOTAL'),'STATUS',\
col('HOURS_INDIRECT').cast(DoubleType()).alias('HOURS_INDIRECT'),\
col('HOURS_TIMED').cast(DoubleType()).alias('HOURS_TIMED'), 'TAC_CODE')\
.withColumn('DIRECT_INDIRECT', when(col('HOURS_INDIRECT')>0, 'INDIRECT').otherwise('DIRECT'))\
.withColumn('KEY_COL', concat(col('TRANSACTION_DATE'), lit('-'),col('EMPLOYEE_NUMBER')))\
.withColumn('WEEK_NUMBER', weekofyear(col('TRANSACTION_DATE')))\
.distinct()

Jas_Labour_Paid_df=Jas_Labour_Paid_df.filter(Jas_Labour_Paid_df['TRANSACTION_DATE'] >= lit("2019-01-01"))\
.filter(Jas_Labour_Paid_df['TRANSACTION_DATE'] <= lit("2019-02-21"))
##Jas_Labour_Paid_df.printSchema()

##Performing Transformations on Jas_Labour_Production_df
##Jas_Labour_Production_df.printSchema()

##Jas_Labour_Production_df.select('START_TIME').distinct().show()
Jas_Labour_Production_df=Jas_Labour_Production_df.select(col('EXTERNAL_ID').alias('EMPLOYEE_NUMBER1'),\
col('USER_NAME').alias('EMPLOYEE_NAME1'),\
col('ATTENTION').alias('ATTENTION1'),\
trim(split(col('DEPT_NAME'),'-')[0]).alias('PROGRAM_DESC1'),\
col('SHIFT_NAME'), to_date(substring(col('ENTRY_DATE'),1,9),'dd-MMM-yy').alias('ENTRY_DATE'),\
to_date(substring(col('START_TIME'),1,9),'dd-MMM-yy').alias('TRANSACTION_DATE1'),\
to_date(substring(col('STOP_TIME'),1,9),'dd-MMM-yy').alias('STOP_TIME'),\
col('HOURS').cast(DoubleType()).alias('PROD_REG_HRS'),\
col('OVER_TIME').cast(DoubleType()).alias('PROD_OT_HRS'),\
col('HOURS_BILLABLE').cast(DoubleType()).alias('HOURS_BILLABLE'),\
col('BURDEN_RATE').cast(DoubleType()).alias('BURDEN_RATE'))\
.withColumn('WEEK_NUM', weekofyear(col('TRANSACTION_DATE1')))\
.withColumn('KEY_COL1', concat(col('TRANSACTION_DATE1'),lit('-'), col('EMPLOYEE_NUMBER1')))\
.distinct()

##Jas_Labour_Production_df.select('HOURS_BILLABLE').distinct().show()
Jas_Labour_Production_df=Jas_Labour_Production_df.filter(Jas_Labour_Production_df['TRANSACTION_DATE1'] >= lit("2019-01-01"))\
.filter(Jas_Labour_Production_df['TRANSACTION_DATE1'] <= lit("2019-02-21"))
##Jas_Labour_Production_df.printSchema()

##Performing join operation between two given sources and storing it in an intermediate dataframe
Jas_Labour_Interm_df=Jas_Labour_Paid_df.join(Jas_Labour_Production_df,Jas_Labour_Paid_df.KEY_COL==Jas_Labour_Production_df.KEY_COL1,'inner')\
.select('TRANSACTION_DATE','EMPLOYEE_NUMBER','KEY_COL','EMPLOYEE_NAME','PROGRAM_DESC','AVAIL_OT_HRS','WEEK_NUMBER','DIRECT_INDIRECT',\
'HOURS_TOTAL',Jas_Labour_Production_df['PROD_REG_HRS'],Jas_Labour_Production_df['PROD_OT_HRS'],Jas_Labour_Production_df['HOURS_BILLABLE'])

##Final dataframe with the required columns
Jas_Labour_Final_df=Jas_Labour_Interm_df\
.withColumn('AVAIL_REG_HRS', when(col('HOURS_BILLABLE') !=0, col('HOURS_BILLABLE')).otherwise(0))\
.withColumn('EXP_REG_HRS',\
when(((col('HOURS_TOTAL') !=0) & (col('PROD_REG_HRS')!=0)),greatest(col('HOURS_TOTAL'),col('PROD_REG_HRS'))).when(((col('HOURS_TOTAL')!=0) & (col('PROD_REG_HRS') == 0)),col('HOURS_TOTAL'))\
.when(((col('HOURS_TOTAL')==0) & (col('PROD_REG_HRS') !=0)), col('PROD_REG_HRS')))\
.withColumn('RATE_TYPE', lit('JAS'))\
.withColumn('RATE_VALUE', lit(80))\
.withColumn('COMPANY_CODE', lit(1))\
.withColumn('SOURCE', lit('Quantum'))

##Jas_Labour_Final_df.printSchema()
Jas_Labour_Final_df=Jas_Labour_Final_df.select('TRANSACTION_DATE','EMPLOYEE_NUMBER',col('DIRECT_INDIRECT').alias('JOB_CATEGORY'),\
'PROGRAM_DESC','COMPANY_CODE',col('WEEK_NUMBER').alias('WEEK_NO'),'SOURCE',col('HOURS_TOTAL').alias('TOTAL_REG_HOURS'),\
col('AVAIL_OT_HRS').alias('TOTAL_OT_HOURS'),col('PROD_REG_HRS').alias('CHARGEABLE_REG_HOURS'),col('PROD_OT_HRS').alias('CHARGEABLE_OT_HOURS'),\
col('EXP_REG_HRS').alias('NON_CHARGEABLE_REG_HOURS'))\
.withColumn('EMPLOYEE_BASE', lit('N/A'))\
.withColumn('BASE_DESCRIPTION', lit('N/A'))\
.withColumn('SHOP_CODE', lit(0))\
.withColumn('SHOP_DESCRIPTION', lit('N/A'))\
.withColumn('EMPLOYEE_STATUS', lit('N/A'))\
.withColumn('WORK_STATUS', lit('N/A'))\
.withColumn('AVAILABLE_FOR_WORK', lit('N/A'))\
.withColumn('WORK_CODE_TYPE', lit('N/A'))\
.withColumn('NON_CHARGEABLE_OT_HOURS', lit(0))

Jas_Labour_Final_df=Jas_Labour_Final_df.select('TRANSACTION_DATE','EMPLOYEE_NUMBER','EMPLOYEE_BASE','BASE_DESCRIPTION','JOB_CATEGORY',\
'PROGRAM_DESC','COMPANY_CODE','SHOP_CODE','SHOP_DESCRIPTION','WEEK_NO','SOURCE','EMPLOYEE_STATUS','WORK_STATUS','AVAILABLE_FOR_WORK',\
'WORK_CODE_TYPE','TOTAL_REG_HOURS','TOTAL_OT_HOURS','NON_CHARGEABLE_REG_HOURS','NON_CHARGEABLE_OT_HOURS','CHARGEABLE_REG_HOURS','CHARGEABLE_OT_HOURS')

Jas_Labour_Final_dyf=DynamicFrame.fromDF(Jas_Labour_Final_df,glueContext,"Jas_Labour_Final_dyf")
##Jas_Labour_Final_dyf.write.format('csv').save("s3://smartstagingbucket/staging_aero/")
glueContext.write_dynamic_frame.from_options(
          frame = Jas_Labour_Final_dyf,
          connection_type = "s3",
          connection_options = {"path": "s3://smartstagingbucket/staging_aero/"},
          format = "csv")