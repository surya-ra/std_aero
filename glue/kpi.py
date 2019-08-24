# Project Title : STD AERO POC- populate RDS
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


#initiate glue context
glueContext = GlueContext(SparkContext.getOrCreate())


#read the source table
kpi_creation_dyf = glueContext.create_dynamic_frame.from_catalog(
       database = "stg_consolidate_aero",
       table_name = "staging_aero")

#convert to dataframes
kpi_creation_df = kpi_creation_dyf.toDF()

#df for NAVIXA
kpi_creation_navixa_df = kpi_creation_df.filter("source == 'NAVIXA'")

#df for RPA
kpi_creation_rpa_df = kpi_creation_df.filter("source == 'RPA'")

kpi_creation_quantum_df = kpi_creation_df.filter("source == 'Quantum'")

#------------------------------------------------NAVIXA--------------------------------------------------------------#
#----------------------------------------KPI creation starts here---------------------------------------------------#
#1. KPI-- EMPLOYEES_AVAILABLE
#   -- If available =>0 else => 1
#   -- Only for NAVIXA
#   -- for RPA => "NA"
kpi_creation_navixa_df = kpi_creation_navixa_df.withColumn('EMPLOYEES_AVAILABLE', expr(
    """IF(available_for_work = 'Y', 0 , 1)"""
))


#2. KPI-- EMPLOYEE_AVAILABILITY_HOURS
#2.1. Create a runtime lookup table with group by
#2.1.1. Create Key by concat(transaction_date,employee_number,EMPLOYEES_AVAILABLE)
kpi_creation_navixa_df = kpi_creation_navixa_df.withColumn('key_agg',concat('transaction_date',lit('_'),'employee_number',lit('_'),'EMPLOYEES_AVAILABLE'))
#2.1.2. Filter the data frame with EMPLOYEES_AVAILABLE = 0
navixa_interim_lookup_avl_emp_df = kpi_creation_navixa_df.filter("EMPLOYEES_AVAILABLE == '0'")
#2.1.3. Create a lookup frame by grouping the data
navixa_interim_lookup_avl_hrs_df = navixa_interim_lookup_avl_emp_df.groupBy(['key_agg']).agg(sum('total_reg_hours').alias('final_column'),sum('chargeable_reg_hours').alias('t_chargeable_reg_hours'),sum('chargeable_ot_hours').alias('t_chargeable_ot_hours'),sum('non_chargeable_reg_hours').alias('t_non_chargeable_reg_hours'),sum('non_chargeable_ot_hours').alias('t_non_chargeable_ot_hours'))

#2.2. Join the look up table with the master dataframe kpi_creation_df
tx = kpi_creation_navixa_df.alias('tx')
ty = navixa_interim_lookup_avl_hrs_df.alias('ty')
#2.2.1. join condition
kpi_creation_navixa_df = tx.join(ty,col('ty.key_agg') == col('tx.key_agg'),'left').select([col('tx.'+xx) for xx in tx.columns] + [col('ty.final_column').alias('EMPLOYEE_AVAILABILITY_HOURS')])


#3. KPI-- UTILIZATION_RATIO
navixa_kpi_util_ratio_interim_df = navixa_interim_lookup_avl_hrs_df.withColumn('UTILIZATION_RATIO',((f.col('t_chargeable_reg_hours')+f.col('t_chargeable_ot_hours'))/(f.col('t_chargeable_reg_hours')+f.col('t_chargeable_ot_hours')+f.col('t_non_chargeable_reg_hours')+f.col('t_non_chargeable_ot_hours'))))

ta = kpi_creation_navixa_df.alias('ta')
tb = navixa_kpi_util_ratio_interim_df.alias('tb')

kpi_creation_navixa_df = ta.join(tb,col('tb.key_agg') == col('ta.key_agg'),'left').select([col('ta.'+xx) for xx in ta.columns] + [col('tb.UTILIZATION_RATIO').alias('UTILIZATION_RATIO')])


#4. KPI-- NON-PRODUCTIVE-TIME
#4.1. create interim df with work code type
navixa_kpi_non_prod_ratio_interim_df = navixa_interim_lookup_avl_emp_df.groupBy(['key_agg','work_code_type']).agg(sum('total_reg_hours').alias('final_column'),sum('chargeable_reg_hours').alias('t_chargeable_reg_hours'),sum('chargeable_ot_hours').alias('t_chargeable_ot_hours'),sum('non_chargeable_reg_hours').alias('t_non_chargeable_reg_hours'),sum('non_chargeable_ot_hours').alias('t_non_chargeable_ot_hours'))
#4.2. aggregation values for work_code_type = 'N'
navixa_kpi_non_prod_ratio_interim_df = navixa_kpi_non_prod_ratio_interim_df.withColumn('neum',expr("""IF(work_code_type = 'N', t_chargeable_reg_hours + t_chargeable_ot_hours + t_non_chargeable_reg_hours + t_non_chargeable_ot_hours, 'NA')"""))
#4.3. aggregation values for all work codes
navixa_kpi_non_prod_ratio_interim_df = navixa_kpi_non_prod_ratio_interim_df.withColumn('denom',(f.col('t_chargeable_reg_hours')+f.col('t_chargeable_ot_hours')+f.col('t_non_chargeable_reg_hours')+f.col('t_non_chargeable_ot_hours')))
#4.4. ratio calculation
navixa_kpi_non_prod_ratio_interim_df = navixa_kpi_non_prod_ratio_interim_df.withColumn('NON_PRODUCTIVE_TIME',(f.col('neum')/f.col('denom')))
#4.5. join logic
ta_n = kpi_creation_navixa_df.alias('ta_n')
tb_n = navixa_kpi_non_prod_ratio_interim_df.alias('tb_n')
kpi_creation_navixa_df = ta_n.join(tb_n,col('tb_n.key_agg') == col('ta_n.key_agg'),'left').select([col('ta_n.'+xx) for xx in ta_n.columns] + [col('tb_n.NON_PRODUCTIVE_TIME').alias('NON_PRODUCTIVE_TIME')])


#5. KPI-- REGULAR-HOURS-MORE-THAN-8
kpi_creation_navixa_df = kpi_creation_navixa_df.withColumn('REG_HRS_MORE_NORM',expr("""IF(EMPLOYEES_AVAILABLE > 8, EMPLOYEES_AVAILABLE-8, 'NA')"""))

#6. KPI-- OVERTIME-RATIO
kpi_creation_navixa_df = kpi_creation_navixa_df.withColumn('OVERTIME_RATIO', expr("""IF(available_for_work = 'Y', (chargeable_ot_hours + non_chargeable_ot_hours)/(chargeable_ot_hours + non_chargeable_ot_hours + non_chargeable_reg_hours + chargeable_reg_hours) , 'NA')"""))

#7. KPI-- CHARGABILITY_HOUR
kpi_creation_navixa_df = kpi_creation_navixa_df.withColumn('CHARGABILITY_HOUR', f.col('chargeable_ot_hours') + f.col('chargeable_reg_hours'))


#------------------------------------------------RPA--------------------------------------------------------------#
#----------------------------------------KPI creation starts here---------------------------------------------------#

#1. KPI-- EMPLOYEES_AVAILABLE
kpi_creation_rpa_df = kpi_creation_rpa_df.withColumn('EMPLOYEES_AVAILABLE',lit(None))
#d_new = kpi_creation_rpa_df.alias('d_new')

#3. Create Key column
kpi_creation_rpa_df = kpi_creation_rpa_df.withColumn('key_agg',concat('transaction_date',lit('_'),'employee_number'))


#4. KPI-- EMPLOYEE_AVAILABILITY_HOURS
#4.1.Lookup table with aggregate of available hours
rpa_interim_lookup_avl_hrs_df = kpi_creation_rpa_df.groupBy(['key_agg']).agg(sum('total_reg_hours').alias('t_avl_reg_hrs'),sum('total_ot_hours').alias('t_avl_ot_hrs'),sum('chargeable_reg_hours').alias('t_prod_reg_hours'),sum('chargeable_ot_hours').alias('t_prod_ot_hours'),sum('non_chargeable_reg_hours').alias('t_exp_reg_hours'),sum('non_chargeable_ot_hours').alias('t_exp_ot_hours'))
#4.3.Perform join 
r_ta_avl_hrs = kpi_creation_rpa_df.alias('r_ta_avl_hrs')
r_tb_avl_hrs = rpa_interim_lookup_avl_hrs_df.alias('r_tb_avl_hrs')
kpi_creation_rpa_df = r_ta_avl_hrs.join(r_tb_avl_hrs,col('r_tb_avl_hrs.key_agg') == col('r_ta_avl_hrs.key_agg'),'left').select([col('r_ta_avl_hrs.'+xx) for xx in r_ta_avl_hrs.columns] + [col('r_tb_avl_hrs.t_avl_reg_hrs').alias('EMPLOYEE_AVAILABILITY_HOURS')])


#5. KPI-- UTILIZATION_RATIO
#5.2.Calculate Utilization hours
rpa_kpi_util_ratio_interim_df = rpa_interim_lookup_avl_hrs_df.withColumn('UTILIZATION_RATIO',((f.col('t_prod_reg_hours')+f.col('t_prod_ot_hours'))/(f.col('t_avl_reg_hrs')+f.col('t_avl_ot_hrs'))))
#5.3.Perform join 
r_ta = kpi_creation_rpa_df.alias('r_ta')
r_tb = rpa_kpi_util_ratio_interim_df.alias('r_tb')
kpi_creation_rpa_df = r_ta.join(r_tb,col('r_tb.key_agg') == col('r_ta.key_agg'),'left').select([col('r_ta.'+xx) for xx in r_ta.columns] + [col('r_tb.UTILIZATION_RATIO').alias('UTILIZATION_RATIO')])


#6. KPI-- NON-PRODUCTIVE-TIME
#6.1.Calculate NON-PRODUCTIVE-HOURS
rpa_kpi_non_prod_interim_df = rpa_interim_lookup_avl_hrs_df.withColumn('NON_PRODUCTIVE_TIME',((f.col('t_exp_reg_hours')+f.col('t_exp_ot_hours'))/(f.col('t_avl_reg_hrs')+f.col('t_avl_ot_hrs'))))
#6.2.Perform join 
r_ta_p = kpi_creation_rpa_df.alias('r_ta_p')
r_tb_p = rpa_kpi_non_prod_interim_df.alias('r_tb_p')
kpi_creation_rpa_df = r_ta_p.join(r_tb_p,col('r_tb_p.key_agg') == col('r_ta_p.key_agg'),'left').select([col('r_ta_p.'+xx) for xx in r_ta_p.columns] + [col('r_tb_p.NON_PRODUCTIVE_TIME').alias('NON_PRODUCTIVE_TIME')])


#7. KPI-- OVERTIME-RATIO
#7.1.Calculate NON-PRODUCTIVE-HOURS
kpi_creation_rpa_df = kpi_creation_rpa_df.withColumn('OVERTIME_RATIO',(f.col('chargeable_ot_hours')+f.col('non_chargeable_ot_hours'))/(f.col('total_reg_hours')+f.col('total_ot_hours')))
#kpi_creation_rpa_df = kpi_creation_rpa_df.withColumn('OVERTIME_RATIO',lit('fill_val'))
#7.2.Perform join 
#r_ta_ot = kpi_creation_rpa_df.alias('r_ta_ot')
#r_tb_ot = rpa_kpi_ot_ratio_interim_df.alias('r_tb_ot')
#kpi_creation_rpa_df = r_ta_ot.join(r_tb_ot,col('r_tb_ot.key_agg') == col('r_ta_ot.key_agg'),'left').select([col('r_ta_ot.'+xx) for xx in r_ta_ot.columns] + [col('r_tb_ot.OVERTIME_RATIO').alias('OVERTIME_RATIO')])


#8. KPI-- REGULAR-HOURS-MORE-THAN-8
kpi_creation_rpa_df = kpi_creation_rpa_df.withColumn('REG_HRS_MORE_NORM',expr("""IF(EMPLOYEE_AVAILABILITY_HOURS > 8, EMPLOYEE_AVAILABILITY_HOURS-8, 'NA')"""))

#9. KPI-- CHARGABILITY_HOUR
kpi_creation_rpa_df = kpi_creation_rpa_df.withColumn('CHARGABILITY_HOUR', f.col('chargeable_reg_hours') + f.col('chargeable_ot_hours'))


#-----------------------------------------------------Quantum----------------------------------------------##

kpi_creation_quantum_interim_df = kpi_creation_quantum_df.withColumn('UTILIZATION_RATIO',((f.col('chargeable_reg_hours')+f.col('chargeable_ot_hours'))/(f.col('chargeable_reg_hours')+f.col('chargeable_ot_hours')+f.col('non_chargeable_reg_hours')+f.col('non_chargeable_ot_hours'))))

kpi_creation_quantum_interim_df = kpi_creation_quantum_interim_df.withColumn('EMPLOYEES_AVAILABLE',lit(None))\
                                    .withColumn('EMPLOYEE_AVAILABILITY_HOURS',lit(None))\
    .withColumn('NON_PRODUCTIVE_TIME',lit(None))\
    .withColumn('OVERTIME_RATIO',lit(None))\
    .withColumn('CHARGABILITY_HOUR',lit(None))\
.withColumn('key_agg',lit(None))\
.withColumn('REG_HRS_MORE_NORM',lit(None))
#---------------------------------------------------END OF KPI CREATION LOGICS--------------------------------------------#

#Augment the two frames
kpi_creation_df = kpi_creation_navixa_df.unionAll(kpi_creation_rpa_df)
kpi_creation_df_final = kpi_creation_df.unionAll(kpi_creation_quantum_interim_df)
#kpi_creation_df = kpi_creation_df.unionAll(kpi_creation_rpa_df)

#Drop key column from final frame
kpi_creation_df_final = kpi_creation_df_final.drop('key_agg')

#Alias the column names
kpi_creation_df_final = kpi_creation_df_final.select(col('transaction_date').alias('TRANSACTION_DATE').cast('date'),
                                         col('employee_number').alias('EMPLOYEE_NUMBER').cast('string'),
                                         col('employee_base').alias('EMPLOYEE_BASE').cast('string'),
                                         col('base_description').alias('BASE_DESCRIPTION').cast('string'),
                                         col('job_category').alias('JOB_CATEGORY').cast('string'),
                                         col('program_desc').alias('PROGRAM_DESC').cast('string'),
                                         col('company_code').alias('COMPANY_CODE').cast('string'),
                                         col('shop_code').alias('SHOP_CODE').cast('string'),
                                         col('shop_description').alias('SHOP_DESCRIPTION').cast('string'),
                                         col('week_no').alias('WEEK_NO').cast('long'),
                                         col('source').alias('SOURCE').cast('string'),
                                         col('employee_status').alias('EMPLOYEE_STATUS').cast('string'),
                                         col('work_status').alias('WORK_STATUS').cast('string'),
                                         col('work_code_type').alias('WORK_CODE_TYPE').cast('string'),
                                         col('total_reg_hours').alias('TOTAL_REG_HOURS').cast('float'),
                                         col('total_ot_hours').alias('TOTAL_OT_HOURS').cast('float'),
                                         col('non_chargeable_reg_hours').alias('NON_CHARGEABLE_REG_HOURS').cast('float'),
                                         col('non_chargeable_ot_hours').alias('NON_CHARGEABLE_OT_HOURS').cast('float'),
                                         col('chargeable_reg_hours').alias('CHARGEABLE_REG_HOURS').cast('float'),
                                         col('chargeable_ot_hours').alias('CHARGEABLE_OT_HOURS').cast('float'),
                                         col('EMPLOYEES_AVAILABLE').alias('EMPLOYEES_AVAILABLE').cast('integer'),
                                         col('EMPLOYEE_AVAILABILITY_HOURS').alias('EMPLOYEE_AVAILABILITY_HOURS').cast('double'),
                                         col('UTILIZATION_RATIO').alias('UTILIZATION_RATIO').cast('double'),
                                         col('NON_PRODUCTIVE_TIME').alias('NON_PRODUCTIVE_TIME').cast('double'),
                                         col('REG_HRS_MORE_NORM').alias('REG_HRS_MORE_NORM').cast('double'),
                                         col('OVERTIME_RATIO').alias('OVERTIME_RATIO').cast('double'),
                                         col('CHARGABILITY_HOUR').alias('CHARGABILITY_HOUR').cast('double')
                                        )

kpi_creation_df_final = kpi_creation_df_final.withColumn('OVERTIME_RATIO',expr("""IF(SOURCE == 'RPA',IF(OVERTIME_RATIO > 1, (CHARGEABLE_OT_HOURS + NON_CHARGEABLE_OT_HOURS)/(TOTAL_REG_HOURS + TOTAL_OT_HOURS) , OVERTIME_RATIO),OVERTIME_RATIO)"""))


#Push to database

url = "jdbc:oracle:thin:@smartdb-aero-poc.cjm4lbd4tbte.us-east-1.rds.amazonaws.com:1521/orcl"
properties = {
    "user": "*********",
    "password": "**********,
    "driver": "oracle.jdbc.driver.OracleDriver"
}

kpi_creation_df_final.write.jdbc(url = url,mode = "overwrite",table = "AERO_DEMO",properties = properties)


