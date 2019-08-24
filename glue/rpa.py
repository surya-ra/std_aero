# Project Title : STD AERO POC- populate RPA staging bucket
# __authors__ = Suryadeep
# __contact__ = suryadeep.chatterjee@capgemini.com
# __maintainer__ = "developer" 
#_Last_Modified_By = “Suryadeep”
#_Last_Modified_Date= 5/8/2019  

from pyspark.sql.types import IntegerType, StringType, StructType, ArrayType, StructField
from pyspark.sql.functions import *
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

import boto3
import json

#initiate glue context
glueContext = GlueContext(SparkContext.getOrCreate())

#read the source file---RPA
rpa_dyf = glueContext.create_dynamic_frame_from_options(connection_type = "s3", 
                                    connection_options = {"paths": ["s3://smart-ingest-bucket/RPA-source-file/"]}, 
                                    format = "csv",
                                    format_options = {'withHeader' : True})

#read the lookup file---RPA-NAVIXA-AE RO
rpa_AE_RO_dyf = glueContext.create_dynamic_frame_from_options(connection_type = "s3", 
                                    connection_options = {"paths": ["s3://smart-ingest-bucket/Dimension-files/Navixa Employee History_RPA.csv"]}, 
                                    format = "csv",
                                    format_options = {'withHeader' : True})

#read the lookup file---RPA-SUMM-HR
rpa_summ_hr_dyf = glueContext.create_dynamic_frame_from_options(connection_type = "s3", 
                                    connection_options = {"paths": ["s3://smart-ingest-bucket/Dimension-files/SUMM HRWare Employee History_RPA.csv"]}, 
                                    format = "csv",
                                    format_options = {'withHeader' : True})
#convert to dataframes
rpa_AE_RO_df = rpa_AE_RO_dyf.toDF()
rpa_summ_hr_df = rpa_summ_hr_dyf.toDF()
rpa_df = rpa_dyf.toDF()

#-----------------------------------Creation of LOOKUP Dataframes----------------------------------------------#

#1. Transform the date column and create the composite join key---RPA-AE RO
#1.1. Convert CalendarDate to date format from string
#1.1.1. Create UDF function to perform the casting operation(m/dd/yyyy)
func_str_to_date_lookup =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
#1.2.2. Augment the transformed value in rpa_AE_RO_transformed_df
rpa_AE_RO_transformed_df = rpa_AE_RO_df.withColumn('new_date', func_str_to_date_lookup(col('CalendarDate')))
#1.2. Concatenate the new_date column and Employeenumber
rpa_AE_RO_transformed_df = rpa_AE_RO_transformed_df.withColumn('join_key',concat('new_date',lit('_'),'EmployeeNumber'))
#1.3. Drop the field calendar date
rpa_AE_RO_transformed_df = rpa_AE_RO_transformed_df.drop('CalendarDate')

#2. Transform the date column and create the composite join key---RPA-SUMM HR
#2.1. Convert CalendarDate to date format from string
#2.1.1. Create UDF function to perform the casting operation(m/dd/yyyy)
func_str_to_date_lookup_hr =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
#2.2.2. Augment the transformed value in rpa_AE_RO_transformed_df
rpa_summ_hr_transformed_df = rpa_summ_hr_df.withColumn('new_date', func_str_to_date_lookup_hr(col('CalendarDate')))
#2.2. Concatenate the new_date column and Employeenumber
rpa_summ_hr_transformed_df = rpa_summ_hr_transformed_df.withColumn('join_key',concat('new_date',lit('_'),'EmployeeNumber'))
#2.3. Drop the field calendar date
rpa_summ_hr_transformed_df = rpa_summ_hr_transformed_df.drop('CalendarDate')

#Select pertinent columns from the complete data frame
rpa_pertinent_df = rpa_df.select(rpa_df.TRANS_DATE.alias("TRANSACTION_DATE"),
                                       rpa_df.EMP_NO.alias("EMPLOYEE_NUMBER"),
                                       rpa_df.DIRECT_INDIRECT.alias("JOB_CATEGORY"),
                                       rpa_df.PROGRAM_DESC.alias("PROGRAM_DESC"),
                                       rpa_df.COMPANY_CODE.alias("COMPANY_CODE"),
                                       rpa_df.AVAIL_REG_HRS.alias("TOTAL_REG_HOURS"),
                                       rpa_df.AVAIL_OT_HRS.alias("TOTAL_OT_HOURS"),
                                       rpa_df.EXP_REG_HRS.alias("NON_CHARGEABLE_REG_HOURS"),
                                       rpa_df.EXP_OT_HRS.alias("NON_CHARGEABLE_OT_HOURS"),
                                       rpa_df.PROD_REG_HRS.alias("CHARGEABLE_REG_HOURS"),
                                       rpa_df.PROD_OT_HRS.alias("CHARGEABLE_OT_HOURS")
                                      ).withColumn("SOURCE",lit("RPA")).withColumn("AVAILABLE_FOR_WORK",lit("NA")).withColumn("WORK_CODE_TYPE",lit("NA")).withColumn("BASE_DESCRIPTION",lit("NA")).withColumn("SHOP_DESCRIPTION",lit("NA"))

#---------------------------TRANSFORMATION LOGIC STARTS HERE-----------------------------------------------------#

#1. Convert TRANSACTION_DATE to date format from string
#1.1. Remove the extra characters
rpa_pertinent_interim_df = rpa_pertinent_df.withColumn("TRANSACTION_DATE", expr("substring(TRANSACTION_DATE, 1, length(TRANSACTION_DATE)-5)"))
#1.2. Create UDF function to perform the casting operation(mm/dd/yyyy)
func_str_to_date =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
#1.3. Augment the transformed value in rpa_pertinent_interim_df
rpa_pertinent_interim_df = rpa_pertinent_interim_df.withColumn('TRANSACTION_DATE', func_str_to_date(col('TRANSACTION_DATE')))

#2. Replace JOB_CATEGORY field values
#2.1. Convert the characters to all lower case
rpa_pertinent_interim_df = rpa_pertinent_interim_df.withColumn('JOB_CATEGORY',lit(lower(col('JOB_CATEGORY'))))
#2.2. Replace'd' with 'DIRECT' and 'i' with 'INDIRECT'
rpa_pertinent_interim_df = rpa_pertinent_interim_df.withColumn('JOB_CATEGORY', regexp_replace('JOB_CATEGORY','d','DIRECT')).withColumn('JOB_CATEGORY', regexp_replace('JOB_CATEGORY','i','INDIRECT'))

#3. Create the join_key by concatinating transaction date and employee id
rpa_pertinent_interim_df = rpa_pertinent_interim_df.withColumn('join_key',concat('TRANSACTION_DATE',lit('_'),'EMPLOYEE_NUMBER'))

#4. Transformation logic to get EMPLOYEE_STATUS
#4.1. look up the AE_RO dataframe to get employee status
a = rpa_pertinent_interim_df.alias('a')
b = rpa_AE_RO_transformed_df.alias('b')
rpa_AE_RO_join_df = a.join(b,col('b.join_key') == col('a.join_key'),'left').select([col('a.'+xx) for xx in a.columns] + [col('b.EmployeeStatus').alias('EMPLOYEE_STATUS')])

#4.2. Filter out a dataframe with null values for employee status
rpa_AE_RO_null_df = rpa_AE_RO_join_df.where(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in rpa_AE_RO_join_df.columns)))

#4.3. Subset dataframe with no null values for employee status
rpa_AE_RO_value_df = rpa_AE_RO_join_df.filter("EMPLOYEE_STATUS != 'null'")
#4.4. Drop the Employee status field from dataframe
rpa_AE_RO_null_df = rpa_AE_RO_null_df.drop('EMPLOYEE_STATUS')

#4.5. Lookup from Summ_hr dataframe on the subset dataframe with null values
x = rpa_AE_RO_null_df.alias('x')
y = rpa_summ_hr_transformed_df.alias('y')
rpa_summ_hr_join_df = x.join(y,col('y.join_key') == col('x.join_key'),'left').select([col('x.'+xx) for xx in x.columns] + [col('y.EmployeeStatus').alias('EMPLOYEE_STATUS')])
#4.6. Filter not null values after joining
rpa_summ_hr_value_df = rpa_summ_hr_join_df.filter("EMPLOYEE_STATUS != 'null'")
#4.7. Merge the subsets to eliminate null values
rpa_pertinent_interim_df_new = rpa_AE_RO_value_df.unionAll(rpa_summ_hr_value_df)

#5. Transformation logic for WORK_STATUS
tx = rpa_pertinent_interim_df_new.alias('tx')
ty = rpa_summ_hr_transformed_df.alias('ty')
tz = rpa_AE_RO_transformed_df.alias('tz')

#5.1. Join with lookup table to get WORK_STATUS
rpa_pertinent_interim_df_new = tx.join(ty,col('ty.join_key') == col('tx.join_key'),'left').select([col('tx.'+xx) for xx in tx.columns] + [col('ty.workStatus').alias('WORK_STATUS')])
#5.2. Filter out the null values from WORK_STATUS
rpa_pertinent_interim_df_new = rpa_pertinent_interim_df_new.filter("WORK_STATUS != 'null'")
tab= rpa_pertinent_interim_df_new.alias('tab')
#6. Create look up for basecode and shop code
#6.1. Join with lookup table to get WORK_STATUS
rpa_pertinent_interim_df_new = tab.join(tz,col('tz.join_key') == col('tab.join_key'),'left').select([col('tab.'+xx) for xx in tab.columns] + [col('tz.Basecode').alias('EMPLOYEE_BASE'),col('tz.ShopCode').alias('SHOP_CODE')])

#7. Get week_number from transaction date
rpa_pertinent_interim_df_new = rpa_pertinent_interim_df_new.withColumn('WEEK_NO',weekofyear('TRANSACTION_DATE'))

#8. Aggregate hour counts
rpa_pertinent_interim_df_new = rpa_pertinent_interim_df_new.groupBy(['TRANSACTION_DATE','EMPLOYEE_NUMBER','EMPLOYEE_BASE','BASE_DESCRIPTION','JOB_CATEGORY','PROGRAM_DESC','COMPANY_CODE','SHOP_CODE','SHOP_DESCRIPTION','WEEK_NO','SOURCE','EMPLOYEE_STATUS','WORK_STATUS','AVAILABLE_FOR_WORK','WORK_CODE_TYPE']).agg(sum("TOTAL_REG_HOURS").alias("TOTAL_REG_HOURS"),sum("TOTAL_OT_HOURS").alias("TOTAL_OT_HOURS"),sum("NON_CHARGEABLE_REG_HOURS").alias("NON_CHARGEABLE_REG_HOURS"),sum("NON_CHARGEABLE_OT_HOURS").alias("NON_CHARGEABLE_OT_HOURS"),sum("CHARGEABLE_REG_HOURS").alias("CHARGEABLE_REG_HOURS"),sum("CHARGEABLE_OT_HOURS").alias("CHARGEABLE_OT_HOURS"))
#9. Cast EMPLOYEE_NUMBER
rpa_pertinent_interim_df_new = rpa_pertinent_interim_df_new.withColumn('EMPLOYEE_NUMBER', rpa_pertinent_interim_df_new.EMPLOYEE_NUMBER.cast('bigint'))

#----------------------------------------END OF TRANSFORMATION LOGIC-----------------------------------#

#9. Convert dataframe to dynamic frame
rpa_pertinent_interim_dyf_new = DynamicFrame.fromDF(rpa_pertinent_interim_df_new, glueContext, "nested")

#10. Push dynamicframe to Staging s3 bucket
glueContext.write_dynamic_frame.from_options(
          frame = rpa_pertinent_interim_dyf_new,
          connection_type = "s3",
          connection_options = {"path": "s3://smartstagingbucket/staging_aero/"},
          format = "csv")


