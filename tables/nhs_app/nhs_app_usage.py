# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_nhs_app_usage.py
DESCRIPTION:
                Databricks notebook to create nhs_app usage table for dashboard
USAGE:
                ...
CONTRIBUTORS:    Oliver Jones
CONTACT:        nhsx.data@england.nhs.uk 
CREATED:        8 Aug 2022
VERSION:        0.0.1
"""

# COMMAND ----------

# MAGIC %pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.*

# COMMAND ----------

from datetime import datetime
import json
import io
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd

# COMMAND ----------

# MAGIC %run /Shared/databricks/au-azure-databricks-cicd/functions/dbrks_helper_functions

# COMMAND ----------

##Getconfiguration
print("Start reading configuration file")
print("--------------------------------")

config_file_path = "/config/pipelines/nhsx-au-analytics/"
config_file_name = "config_nhs_app_dbrks.json"
base_path = "proc/projects/nhsx_slt_analytics/national_digital_channels/nhs_app/" 

CONNECTION_STRING = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONNECTION_STRING")
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, config_file_path, config_file_name,)
config_JSON = json.loads(io.BytesIO(config_JSON).read())
print("Finish reading configuration file")

#Get latest folder
print("Getting latest folder")
print("--------------------------------")

source_path = config_JSON['pipeline']['project']['source_path']
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
print("Finish getting latest folder")



# COMMAND ----------

#Create pandas dataframe for the tables

# nhs_app_usage_logins_day_count 
logins_data_dir = config_JSON['pipeline']['project']['databricks'][6]['sink_path']
logins_file_name = config_JSON['pipeline']['project']['databricks'][6]['sink_file']

print("Loaction and file name for nhs_app_usage_logins_day_count")
print(logins_data_dir)
print(logins_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, logins_data_dir+latestFolder, logins_file_name)
df_logins = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_repeat_prescriptions_day_count 
prescriptions_data_dir = config_JSON['pipeline']['project']['databricks'][9]['sink_path']
prescriptions_file_name = config_JSON['pipeline']['project']['databricks'][9]['sink_file']

print("Loaction and file name for nhs_app_usage_repeat_prescriptions_day_count")
print(prescriptions_data_dir)
print(prescriptions_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, prescriptions_data_dir+latestFolder, prescriptions_file_name)
df_prescriptions = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_primary_care_appointments_booked_day_count 
appts_booked_data_dir = config_JSON['pipeline']['project']['databricks'][7]['sink_path']
appts_booked_file_name = config_JSON['pipeline']['project']['databricks'][7]['sink_file']

print("Loaction and file name for nhs_app_usage_primary_care_appointments_booked_day_count")
print(appts_booked_data_dir)
print(appts_booked_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, appts_booked_data_dir+latestFolder, appts_booked_file_name)
df_appts_booked = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_primary_care_appointments_cancelled_day_count 
appts_can_data_dir = config_JSON['pipeline']['project']['databricks'][8]['sink_path']
appts_can_file_name = config_JSON['pipeline']['project']['databricks'][8]['sink_file']

print("Loaction and file name for nhs_app_usage_primary_care_appointments_cancelled_day_count")
print(appts_can_data_dir)
print(appts_can_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, appts_can_data_dir+latestFolder, appts_can_file_name)
df_appts_can = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_organ_donation_lookup_day_count 
organ_donation_lookup_data_dir = config_JSON['pipeline']['project']['databricks'][16]['sink_path']
organ_donation_lookup_file_name = config_JSON['pipeline']['project']['databricks'][16]['sink_file']

print("Loaction and file name for nhs_app_usage_organ_donation_lookup_day_count")
print(organ_donation_lookup_data_dir)
print(organ_donation_lookup_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, organ_donation_lookup_data_dir+latestFolder, organ_donation_lookup_file_name)
df_organ_donation_lookup = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_organ_donation_registration_day_count 
organ_donation_registration_data_dir = config_JSON['pipeline']['project']['databricks'][13]['sink_path']
organ_donation_registration_file_name = config_JSON['pipeline']['project']['databricks'][13]['sink_file']

print("Loaction and file name for nhs_app_usage_organ_donation_registration_day_count")
print(organ_donation_registration_data_dir)
print(organ_donation_registration_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, organ_donation_registration_data_dir+latestFolder, organ_donation_registration_file_name)
df_organ_donation_registration = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_organ_donation_update_day_count 
organ_donation_update_data_dir = config_JSON['pipeline']['project']['databricks'][15]['sink_path']
organ_donation_update_file_name = config_JSON['pipeline']['project']['databricks'][15]['sink_file']

print("Loaction and file name for nhs_app_usage_organ_donation_update_day_count")
print(organ_donation_update_data_dir)
print(organ_donation_update_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, organ_donation_update_data_dir+latestFolder, organ_donation_update_file_name)
df_organ_donation_update = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_organ_donation_withdrawal_day_count 
organ_donation_withdrawal_data_dir = config_JSON['pipeline']['project']['databricks'][14]['sink_path']
organ_donation_withdrawal_file_name = config_JSON['pipeline']['project']['databricks'][14]['sink_file']

print("Loaction and file name for nhs_app_usage_organ_donation_withdrawal_day_count")
print(organ_donation_withdrawal_data_dir)
print(organ_donation_withdrawal_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, organ_donation_withdrawal_data_dir+latestFolder, organ_donation_withdrawal_file_name)
df_organ_donation_withdrawal = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_record_views_day_count 
record_views_data_dir = config_JSON['pipeline']['project']['databricks'][10]['sink_path']
record_views_file_name = config_JSON['pipeline']['project']['databricks'][10]['sink_file']

print("Loaction and file name for nhs_app_usage_record_views_day_count")
print(record_views_data_dir)
print(record_views_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, record_views_data_dir+latestFolder, record_views_file_name)
df_record_views = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_summary_record_views_day_count 
summary_record_views_data_dir = config_JSON['pipeline']['project']['databricks'][11]['sink_path']
summary_record_views_file_name = config_JSON['pipeline']['project']['databricks'][11]['sink_file']

print("Loaction and file name for nhs_app_usage_summary_record_views_day_count")
print(summary_record_views_data_dir)
print(summary_record_views_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, summary_record_views_data_dir+latestFolder, summary_record_views_file_name)
df_summary_record_views = pd.read_csv(io.BytesIO(prop_data))

# nhs_app_usage_detail_coded_record_views_day_count 
detail_coded_record_views_data_dir = config_JSON['pipeline']['project']['databricks'][12]['sink_path']
detail_coded_record_views_file_name = config_JSON['pipeline']['project']['databricks'][12]['sink_file']

print("Loaction and file name for nhs_app_usage_detail_coded_record_views_day_count")
print(detail_coded_record_views_data_dir)
print(detail_coded_record_views_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, detail_coded_record_views_data_dir+latestFolder, detail_coded_record_views_file_name)
df_detail_coded_record_views = pd.read_csv(io.BytesIO(prop_data))

# COMMAND ----------

#Create spark dataframe
print("Creating Spark dataframes")
print("--------------------------------")

df_logins = spark.createDataFrame(df_logins)
df_prescriptions = spark.createDataFrame(df_prescriptions)
df_appts_booked = spark.createDataFrame(df_appts_booked)
df_appts_can = spark.createDataFrame(df_appts_can)
df_organ_donation_lookup = spark.createDataFrame(df_organ_donation_lookup)
df_organ_donation_registration = spark.createDataFrame(df_organ_donation_registration)
df_organ_donation_update = spark.createDataFrame(df_organ_donation_update)
df_organ_donation_withdrawal = spark.createDataFrame(df_organ_donation_withdrawal)
df_record_views = spark.createDataFrame(df_record_views)
df_summary_record_views = spark.createDataFrame(df_summary_record_views)
df_detail_coded_record_views = spark.createDataFrame(df_detail_coded_record_views)

print("Finish creating dataframe")

#Rename columns with spaces
print("Renaming columns with spaces")
print("--------------------------------")

df_logins = df_logins.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of logins", "Number_of_logins")

df_prescriptions = df_prescriptions.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of repeat prescriptions", "Number_of_repeat_prescriptions")

df_appts_booked = df_appts_booked.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of primary care appointments booked", "Number_of_primary_care_appointments_booked")

df_appts_can = df_appts_can.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of primary care appointments cancelled", "Number_of_primary_care_appointments_cancelled")

df_organ_donation_lookup = df_organ_donation_lookup.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of organ donation lookups", "Number_of_organ_donation_lookups")

df_organ_donation_registration = df_organ_donation_registration.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of organ donation registrations", "Number_of_organ_donation_registrations")

df_organ_donation_update = df_organ_donation_update.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of organ donation updates", "Number_of_organ_donation_updates")

df_organ_donation_withdrawal = df_organ_donation_withdrawal.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of organ donation withdrawals", "Number_of_organ_donation_withdrawals")

df_record_views = df_record_views.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of record views", "Number_of_record_views")

df_summary_record_views = df_summary_record_views.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of summary care record views", "Number_of_summary_care_record_views")

df_detail_coded_record_views = df_detail_coded_record_views.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Number of detail coded record views", "Number_of_detail_coded_record_views")

print("Finish renaming columns")

#Create spark temp views
print("Creating spark views")
print("--------------------------------")

df_logins.createOrReplaceTempView("tv_df_logins")
df_prescriptions.createOrReplaceTempView("tv_df_prescriptions")
df_appts_booked.createOrReplaceTempView("tv_df_appts_booked")
df_appts_can.createOrReplaceTempView("tv_df_appts_can")
df_organ_donation_lookup.createOrReplaceTempView("tv_df_organ_donation_lookup")
df_organ_donation_registration.createOrReplaceTempView("tv_df_organ_donation_registration")
df_organ_donation_update.createOrReplaceTempView("tv_df_organ_donation_update")
df_organ_donation_withdrawal.createOrReplaceTempView("tv_df_organ_donation_withdrawal")
df_record_views.createOrReplaceTempView("tv_df_record_views")
df_summary_record_views.createOrReplaceTempView("tv_df_summary_record_views")
df_detail_coded_record_views.createOrReplaceTempView("tv_df_detail_coded_record_views")

print("Finish creating spark views")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS usage;
# MAGIC 
# MAGIC CREATE TABLE usage 
# MAGIC AS
# MAGIC 
# MAGIC SELECT
# MAGIC 
# MAGIC     a.Date as usage_date
# MAGIC     ,a.Practice_code as usage_practice_code
# MAGIC     
# MAGIC     ,a.Number_of_logins as M0149_number_of_logins
# MAGIC     ,b.Number_of_repeat_prescriptions as M0152_number_of_repeat_prescriptions
# MAGIC     ,c.Number_of_primary_care_appointments_booked as M0150_number_of_primary_care_appointments_booked
# MAGIC     ,d.Number_of_primary_care_appointments_cancelled as M0151_number_of_primary_care_appointments_cancelled
# MAGIC     ,e.Number_of_organ_donation_lookups as M0159_number_of_organ_donation_lookup
# MAGIC     ,f.Number_of_organ_donation_registrations as M0156_number_of_organ_donation_registrations
# MAGIC     ,g.Number_of_organ_donation_updates as M0158_number_of_organ_donation_updates
# MAGIC     ,h.Number_of_organ_donation_withdrawals as M0157_number_of_organ_donation_withdrawals
# MAGIC     ,i.Number_of_record_views as M0153_number_of_record_views
# MAGIC     ,j.Number_of_summary_care_record_views as M0154_number_of_summary_care_record_views
# MAGIC     ,k.Number_of_detail_coded_record_views as M0155_number_of_detail_coded_record_views
# MAGIC 
# MAGIC     ,'1' as JoinCond    
# MAGIC 
# MAGIC FROM
# MAGIC    tv_df_logins a
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_prescriptions b 
# MAGIC ON
# MAGIC     a.Date=b.Date
# MAGIC     AND a.Practice_code=b.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_appts_booked c
# MAGIC ON
# MAGIC     a.Date=c.Date
# MAGIC     AND a.Practice_code=c.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_appts_can d
# MAGIC ON
# MAGIC     a.Date=d.Date
# MAGIC     AND a.Practice_code=d.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_organ_donation_lookup e
# MAGIC ON
# MAGIC     a.Date=e.Date
# MAGIC     AND a.Practice_code=e.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_organ_donation_registration f
# MAGIC ON
# MAGIC     a.Date=f.Date
# MAGIC     AND a.Practice_code=f.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_organ_donation_update g
# MAGIC ON
# MAGIC     a.Date=g.Date
# MAGIC     AND a.Practice_code=g.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_organ_donation_withdrawal h
# MAGIC ON
# MAGIC     a.Date=h.Date
# MAGIC     AND a.Practice_code=h.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_record_views i
# MAGIC ON
# MAGIC     a.Date=i.Date
# MAGIC     AND a.Practice_code=i.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_summary_record_views j
# MAGIC ON
# MAGIC     a.Date=j.Date
# MAGIC     AND a.Practice_code=j.Practice_code
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_df_detail_coded_record_views k
# MAGIC ON
# MAGIC     a.Date=k.Date
# MAGIC     AND a.Practice_code=k.Practice_code
# MAGIC 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS usage_totals;
# MAGIC 
# MAGIC CREATE TABLE usage_totals
# MAGIC as 
# MAGIC select
# MAGIC     usage_practice_code
# MAGIC   
# MAGIC     ,SUM(M0149_number_of_logins) as M0149_number_of_logins
# MAGIC     ,SUM(M0152_number_of_repeat_prescriptions) as M0152_number_of_repeat_prescriptions
# MAGIC     ,SUM(M0150_number_of_primary_care_appointments_booked) as M0150_number_of_primary_care_appointments_booked
# MAGIC     ,SUM(M0151_number_of_primary_care_appointments_cancelled) as M0151_number_of_primary_care_appointments_cancelled
# MAGIC     ,SUM(M0159_number_of_organ_donation_lookup) as M0159_number_of_organ_donation_lookup
# MAGIC     ,SUM(M0156_number_of_organ_donation_registrations) as M0156_number_of_organ_donation_registrations
# MAGIC     ,SUM(M0158_number_of_organ_donation_updates) as M0158_number_of_organ_donation_updates
# MAGIC     ,SUM(M0157_number_of_organ_donation_withdrawals) as M0157_number_of_organ_donation_withdrawals
# MAGIC     ,SUM(M0153_number_of_record_views) as M0153_number_of_record_views
# MAGIC     ,SUM(M0154_number_of_summary_care_record_views) as M0154_number_of_summary_care_record_views
# MAGIC     ,SUM(M0155_number_of_detail_coded_record_views) as M0155_number_of_detail_coded_record_views
# MAGIC from
# MAGIC   usage
# MAGIC group by
# MAGIC   usage_practice_code

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from usage_totals

# COMMAND ----------

print("Creating SQL table in SQL Server")
print("--------------------------------")

table_name = "nhs_app_usage"
df_sql = spark.sql("SELECT * FROM usage")
write_spark_df_to_sql(df_sql, table_name, "overwrite")
spark.sql("DROP TABLE IF EXISTS usage")

print("Finish creating SQL table in SQL Server")


# COMMAND ----------

print("Creating totals SQL table in SQL Server")
print("--------------------------------")

table_name = "nhs_app_usage_totals"
df_sql = spark.sql("SELECT * FROM usage_totals")
write_spark_df_to_sql(df_sql, table_name, "overwrite")
spark.sql("DROP TABLE IF EXISTS usage_totals")

print("Finish creating totals SQL table in SQL Server")
