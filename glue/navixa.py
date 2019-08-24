
# Project Title : STD AERO POC- populate Navixa staging bucket
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

#read the source file---NAVIXA
navixa_dyf = glueContext.create_dynamic_frame_from_options(connection_type = "s3", 
                                    connection_options = {"paths": ["s3://smart-ingest-bucket/Navixa-source-file/"]}, 
                                    format = "csv",
                                    format_options = {'withHeader' : True})

#read the lookup file---NAVIXA-AE RO
navixa_AE_RO_dyf = glueContext.create_dynamic_frame_from_options(connection_type = "s3", 
                                    connection_options = {"paths": ["s3://smart-ingest-bucket/Dimension-files/NavixaEmployeeHistoryAERO.csv"]}, 
                                    format = "csv",
                                    format_options = {'withHeader' : True})

#read the lookup file---NAVIXA-SUMM-HR
navixa_summ_hr_dyf = glueContext.create_dynamic_frame_from_options(connection_type = "s3", 
                                    connection_options = {"paths": ["s3://smart-ingest-bucket/Dimension-files/SUMMHRWareEmployeeHistory.csv"]}, 
                                    format = "csv",
                                    format_options = {'withHeader' : True})
#convert to dataframes
navixa_AE_RO_df = navixa_AE_RO_dyf.toDF()
navixa_summ_hr_df = navixa_summ_hr_dyf.toDF()
navixa_df = navixa_dyf.toDF()

#-----------------------------------Creation of LOOKUP Dataframes----------------------------------------------#

#1. Transform the date column and create the composite join key---NAVIXA-AE RO
#1.1. Substring of the dates
navixa_AE_RO_transformed_df = navixa_AE_RO_df.withColumn('new_date',substring(navixa_AE_RO_df.CalendarDate,1,10))
#1.2. Concatenate the new_date column and Employeenumber
navixa_AE_RO_transformed_df = navixa_AE_RO_transformed_df.withColumn('join_key',concat('new_date',lit('_'),'EmployeeNumber'))
#1.3. Drop the field calendar date
navixa_AE_RO_transformed_df = navixa_AE_RO_transformed_df.drop('CalendarDate')

#2. Transform the date column and create the composite join key---NAVIXA-SUMM-HR
#2.1. Substring of the dates
navixa_summ_hr_transformed_df = navixa_summ_hr_df.withColumn('new_date',substring(navixa_summ_hr_df.CalendarDate,1,10))
#2.2. Concatenate the new_date column and Employeenumber
navixa_summ_hr_transformed_df = navixa_summ_hr_transformed_df.withColumn('join_key',concat('new_date',lit('_'),'EmployeeNumber'))
#2.3. Drop the field calendar date
navixa_summ_hr_transformed_df = navixa_summ_hr_transformed_df.drop('CalendarDate')

#Select pertinent columns from the complete data frame
navixa_pertinent_df = navixa_df.select(navixa_df.LabourTransactionDate.alias("TRANSACTION_DATE"),
                                       navixa_df.EmployeeNumber.alias("EMPLOYEE_NUMBER"),
                                       navixa_df.EmployeeBase.alias("EMPLOYEE_BASE"),
                                       navixa_df.BaseDescription.alias("BASE_DESCRIPTION"),
                                       navixa_df.JobCategory.alias("JOB_CATEGORY"),
                                       navixa_df.ProgramDescription.alias("PROGRAM_DESC"),
                                       navixa_df.SACompanyCode.alias("COMPANY_CODE"),
                                       navixa_df.EmployeeShop.alias("SHOP_CODE"),
                                       navixa_df.ShopDescription.alias("SHOP_DESCRIPTION"),
                                       navixa_df.TotalRegularHours.alias("TOTAL_REG_HOURS"),
                                       navixa_df.TotalOvertimeHours.alias("TOTAL_OT_HOURS"),
                                       navixa_df.NonChargeableRegularHours.alias("NON_CHARGEABLE_REG_HOURS"),
                                       navixa_df.NonChargeableOvertimeHours.alias("NON_CHARGEABLE_OT_HOURS"),
                                       navixa_df.ChargeableRegularHours.alias("CHARGEABLE_REG_HOURS"),
                                       navixa_df.ChargeableOvertimeHours.alias("CHARGEABLE_OT_HOURS"),
                                       navixa_df.AvailableForWork.alias("AVAILABLE_FOR_WORK"),
                                       navixa_df.WorkCodeType.alias("WORK_CODE_TYPE")
                                      ).withColumn("WEEK_NO",lit("NA")).withColumn("SOURCE",lit("NAVIXA"))

#---------------------------TRANSFORMATION LOGIC STARTS HERE-----------------------------------------------------#

#1. Convert TRANSACTION_DATE to date format from string
#1.1. Create UDF function to perform the casting operation(yyyymmdd)
func_str_to_date =  udf (lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
#1.2. Augment the transformed value in navixa_pertinent_interim_df
navixa_pertinent_interim_df = navixa_pertinent_df.withColumn('TRANSACTION_DATE', func_str_to_date(col('TRANSACTION_DATE')))

#2. Replace JOB_CATEGORY field values
navixa_pertinent_interim_df = navixa_pertinent_interim_df.withColumn('JOB_CATEGORY', regexp_replace('JOB_CATEGORY','NotFound','NOT FOUND')).withColumn('JOB_CATEGORY', regexp_replace('JOB_CATEGORY','Direct','DIRECT')).withColumn('JOB_CATEGORY', regexp_replace('JOB_CATEGORY','Indirect','INDIRECT'))

#3. Create the join_key by concatinating transaction date and employee id
navixa_pertinent_interim_df = navixa_pertinent_interim_df.withColumn('join_key',concat('TRANSACTION_DATE',lit('_'),'EMPLOYEE_NUMBER'))

#4. Transformation logic to get EMPLOYEE_STATUS
#4.1. look up the AE_RO dataframe to get employee status
a = navixa_pertinent_interim_df.alias('a')
b = navixa_AE_RO_transformed_df.alias('b')
AE_RO_join_df = a.join(b,col('b.join_key') == col('a.join_key'),'left').select([col('a.'+xx) for xx in a.columns] + [col('b.EmployeeStatus').alias('EMPLOYEE_STATUS')])

#4.2. Filter out a dataframe with null values for employee status
AE_RO_null_df = AE_RO_join_df.where(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in AE_RO_join_df.columns)))
#4.3. Subset dataframe with no null values for employee status
AE_RO_value_df = AE_RO_join_df.filter("EMPLOYEE_STATUS != 'null'")
#4.4. Drop the Employee status field from dataframe
AE_RO_null_df = AE_RO_null_df.drop('EMPLOYEE_STATUS')
#4.5. Lookup from Summ_hr dataframe on the subset dataframe with null values
x = AE_RO_null_df.alias('x')
y = navixa_summ_hr_transformed_df.alias('y')
summ_hr_join_df = x.join(y,col('y.join_key') == col('x.join_key'),'left').select([col('x.'+xx) for xx in x.columns] + [col('y.EmployeeStatus').alias('EMPLOYEE_STATUS')])
#4.6. Filter not null values after joining
summ_hr_value_df = summ_hr_join_df.filter("EMPLOYEE_STATUS != 'null'")
#4.7. Merge the subsets to eliminate null values
navixa_pertinent_interim_df_new = AE_RO_value_df.unionAll(summ_hr_value_df)

#5. Transformation login for WORK_STATUS
tx = navixa_pertinent_interim_df_new.alias('tx')
ty = navixa_summ_hr_transformed_df.alias('ty')
#5.1. Join with lookup table to get WORK_STATUS
navixa_pertinent_interim_df_new = tx.join(ty,col('ty.join_key') == col('tx.join_key'),'left').select([col('tx.'+xx) for xx in tx.columns] + [col('ty.workStatus').alias('WORK_STATUS')])
#5.2. Filter out the null values from WORK_STATUS
navixa_pertinent_interim_df_new = navixa_pertinent_interim_df_new.filter("WORK_STATUS != 'null'")

#6. Aggregate hour counts
navixa_pertinent_interim_df_new = navixa_pertinent_interim_df_new.groupBy(['TRANSACTION_DATE','EMPLOYEE_NUMBER','EMPLOYEE_BASE','BASE_DESCRIPTION','JOB_CATEGORY','PROGRAM_DESC','COMPANY_CODE','SHOP_CODE','SHOP_DESCRIPTION','WEEK_NO','SOURCE','EMPLOYEE_STATUS','WORK_STATUS','AVAILABLE_FOR_WORK','WORK_CODE_TYPE']).agg(sum("TOTAL_REG_HOURS").alias("TOTAL_REG_HOURS"),sum("TOTAL_OT_HOURS").alias("TOTAL_OT_HOURS"),sum("NON_CHARGEABLE_REG_HOURS").alias("NON_CHARGEABLE_REG_HOURS"),sum("NON_CHARGEABLE_OT_HOURS").alias("NON_CHARGEABLE_OT_HOURS"),sum("CHARGEABLE_REG_HOURS").alias("CHARGEABLE_REG_HOURS"),sum("CHARGEABLE_OT_HOURS").alias("CHARGEABLE_OT_HOURS"))

#7. Get week_number from transaction date
navixa_pertinent_interim_final_df = navixa_pertinent_interim_df_new.withColumn('WEEK_NO',weekofyear('TRANSACTION_DATE'))

#8. Cast EMPLOYEE_Number
navixa_pertinent_interim_final_df = navixa_pertinent_interim_final_df.withColumn('EMPLOYEE_NUMBER', navixa_pertinent_interim_final_df.EMPLOYEE_NUMBER.cast('bigint'))
#----------------------------------------END OF TRANSFORMATION LOGIC-----------------------------------#

#8. Convert dataframe to dynamic frame
navixa_pertinent_interim_final_dyf = DynamicFrame.fromDF(navixa_pertinent_interim_final_df, glueContext, "nested")

#9. Push dynamicframe to Staging s3 bucket
glueContext.write_dynamic_frame.from_options(
          frame = navixa_pertinent_interim_final_dyf,
          connection_type = "s3",
          connection_options = {"path": "s3://smartstagingbucket/staging_aero/"},
          format = "csv")
