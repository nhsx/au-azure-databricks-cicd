# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           nhs_app_dct.py
DESCRIPTION:
                Databricks notebook to create nhs_app metrics for DCT
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

# nhs_app_uptake_gp_registered_population_day_prop 
day_prop_data_dir = config_JSON['pipeline']['project']['databricks'][5]['sink_path']
day_prop_file_name = config_JSON['pipeline']['project']['databricks'][5]['sink_file']

print("Loaction and file name for nhs_app_uptake_gp_registered_population_day_prop")
print(day_prop_data_dir)
print(day_prop_file_name)
print("--------------------------------")

prop_data = datalake_download(CONNECTION_STRING, file_system, day_prop_data_dir+latestFolder, day_prop_file_name)
df_day_prop = pd.read_csv(io.BytesIO(prop_data))


#nhs_app_uptake_registrations_day_count
day_count_data_dir = config_JSON['pipeline']['project']['databricks'][2]['sink_path']
day_count_file_name = config_JSON['pipeline']['project']['databricks'][2]['sink_file']

print("Loaction and file name for nhs_app_uptake_registrations_day_count")
print(day_count_data_dir)
print(day_count_file_name)
print("--------------------------------")

count_data = datalake_download(CONNECTION_STRING, file_system, day_count_data_dir+latestFolder, day_count_file_name)
df_day_count = pd.read_csv(io.BytesIO(count_data))


#nhs_app_uptake_p9_registrations_day_count
reg_p9_day_count_data_dir = config_JSON['pipeline']['project']['databricks'][4]['sink_path']
reg_p9_day_count_file_name = config_JSON['pipeline']['project']['databricks'][4]['sink_file']

print("Loaction and file name for nhs_app_uptake_p9_registrations_day_count")
print(reg_p9_day_count_data_dir)
print(reg_p9_day_count_file_name)
print("--------------------------------")

reg_p9_day_count_data = datalake_download(CONNECTION_STRING, file_system, reg_p9_day_count_data_dir+latestFolder, reg_p9_day_count_file_name)
df_reg_p9_day_count = pd.read_csv(io.BytesIO(reg_p9_day_count_data))

# COMMAND ----------

#Create spark dataframe
print("Creating Spark dataframes")
print("--------------------------------")

day_prop_df = spark.createDataFrame(df_day_prop)
day_count_df = spark.createDataFrame(df_day_count)
reg_p9_day_count_df = spark.createDataFrame(df_reg_p9_day_count)

print("Finish creating dataframe")

#Rename columns with spaces
print("Renaming columns with spaces")

print("--------------------------------")
pop_day_prop_df2 = day_prop_df.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Unique ID","Unique_ID")\
.withColumnRenamed("Cumulative number of P9 NHS app registrations", "Cumulative_number_of_P9_NHS_app_registrations")\
.withColumnRenamed("Number of GP registered patients", "Number_of_GP_registered_patients")

day_count_df2 = day_count_df.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Number of NHS app registrations","Number_of_NHS_app_registrations")

reg_p9_day_count_df2 = reg_p9_day_count_df.withColumnRenamed("Practice code","Practice_code")\
.withColumnRenamed("Number of P9 NHS app registrations","Number_of_P9_NHS_app_registrations")

print("Finish renaming columns")

#Create spark temp views
print("Creating spark views")
print("--------------------------------")

pop_day_prop_df2.createOrReplaceTempView("tv_day_prop")
day_count_df2.createOrReplaceTempView("tv_day_count")
reg_p9_day_count_df2.createOrReplaceTempView("tv_reg_p9_day_count")

print("Finish creating spark views")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS uptake;
# MAGIC 
# MAGIC CREATE TABLE uptake 
# MAGIC AS
# MAGIC SELECT
# MAGIC 
# MAGIC     trunc(a.Date, "month") as uptake_date
# MAGIC     ,a.Practice_code as uptake_practice_code	
# MAGIC     ,MAX(a.Number_of_GP_registered_patients) as M0148_number_of_gp_registered_patients
# MAGIC 
# MAGIC     ,SUM(b.Number_of_NHS_app_registrations) as M0144_number_of_nhs_app_registrations
# MAGIC     ,SUM(d.Number_of_P9_NHS_app_registrations) as M0146_number_of_p9_app_registrations
# MAGIC     
# MAGIC  
# MAGIC 
# MAGIC FROM
# MAGIC     tv_day_prop a
# MAGIC LEFT JOIN
# MAGIC  tv_day_count b
# MAGIC ON
# MAGIC  a.Date = b.Date AND a.Practice_code= b.Practice_code
# MAGIC 
# MAGIC 
# MAGIC LEFT JOIN
# MAGIC     tv_reg_p9_day_count d
# MAGIC ON
# MAGIC     a.Date=d.Date
# MAGIC     AND a.Practice_code=d.Practice_code
# MAGIC GROUP BY
# MAGIC uptake_date, a.Practice_code
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uptake

# COMMAND ----------

print("Creating SQL table in SQL Server")
print("--------------------------------")

table_name = "dct_nhs_app"
df_sql = spark.sql("SELECT * FROM uptake")
write_spark_df_to_sql(df_sql, table_name, "overwrite")
spark.sql("DROP TABLE IF EXISTS uptake")

print("Finish creating SQL table in SQL Server")

