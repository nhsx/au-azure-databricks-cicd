# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_trigger_nhs_app_validation.py
DESCRIPTION:
                To reduce manual checks and improve quality validations need to added to dbrks_trigger_nhs_app_devices_validation pipeline for digital social care
USAGE:
                ...
CONTRIBUTORS:   Abdu Nuhu, Kabir Khan, Faaiz Shanawas
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        23 Jan 2023
VERSION:        0.0.1
"""


# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------------------
%pip install pandas pathlib azure-storage-file-datalake numpy pyarrow==5.0.* great_expectations==0.18.* openpyxl

# COMMAND ----------


# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
import json

# 3rd party:
import pandas as pd
import numpy as np
from pathlib import Path
from azure.storage.filedatalake import DataLakeServiceClient
import great_expectations as ge


# COMMAND ----------

# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONNECTION_STRING")


# COMMAND ----------

# MAGIC %run /Shared/databricks/au-azure-databricks-cicd/functions/dbrks_helper_functions

# COMMAND ----------

# Load parameters and JSON config from Azure datalake
# -------------------------------------------------------------------------

file_path_config = "config/pipelines/nhsx-au-analytics"
file_name_config = "config_nhs_app_jumpoff_dbrks.json"
log_table = "dbo.pre_load_log"
agg_log_table = 'dbo.pre_load_agg_log'

file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())


# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON["pipeline"]['raw']["snapshot_source_path"]

# COMMAND ----------

# Pull new snapshot dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
file_name_list = [file for file in file_name_list if 'WeeklyIntegratedPartners' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset))
  new_dataframe['Date'] = pd.to_datetime(new_dataframe['Date']).dt.strftime("%Y-%m-%d")


# COMMAND ----------

# validate data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
val_df = new_dataframe.mask(new_dataframe == " ") # convert all blanks to NaN for validtion
df1 = ge.from_pandas(val_df) # Create great expectations dataframe from pandas datafarme

# COMMAND ----------

#get the sum of the current clicks and todays count
today_count = len(new_dataframe)
sum_clicks = new_dataframe["Clicks"].sum()
print("Today's row count is: " + str(today_count))
print("Today's Clicks sum is: " + str(sum_clicks))

# COMMAND ----------

#get the unique jumpoff counts and provider counts
jumpoff_count = len(new_dataframe['JumpOff'].unique())
provider_count = len(new_dataframe['Provider'].unique())
print("Today's unique jumpoff count is: " + str(jumpoff_count))
print("Today's  unique provider count is: " + str(provider_count))

# COMMAND ----------

#get the unique count of unique jumpoffs from previous weeks data 
print("############# Last run details is shown below ###############################")
previous_jumpoff_count = get_last_agg(agg_log_table, 'WeeklyIntegratedPartners', "count", "count of unique Jump Offs")
display(previous_jumpoff_count)

# COMMAND ----------

#get the unique count of unique providers from previous weeks data
print("############# Last run details is shown below ###############################")
previous_provider_count = get_last_agg(agg_log_table, 'WeeklyIntegratedPartners', "count", "count of unique providers")
display(previous_provider_count)


# COMMAND ----------

#get the unique ODS count from the previous week and calculate the minimum and maximum thresholds
print("############# Last run details is shown below ###############################")
previous_ods_count = get_last_agg(agg_log_table, 'WeeklyIntegratedPartners', "count", "count of unique ODS Codes")
display(previous_ods_count)

#calculate minimum and maximum tolerance values (within 1%)
min_ods_count, max_ods_count = get_thresholds(previous_ods_count['aggregate_value'].values[0], 1)

# COMMAND ----------

#get the sum of the clicks for the previous week's data
last_run = get_last_agg(agg_log_table, 'WeeklyIntegratedPartners', "sum", "sum of the Clicks column")
previous_sum = last_run.iloc[0,3]
print("############# Last run details is shown below ###############################")
display(last_run)

#calculate thresholds using function from helper functions
min_sum_clicks, max_sum_clicks = get_thresholds(previous_sum, 20)

# COMMAND ----------

#get the row count for last weeks data and display the information
print("############# Last run details is shown below ###############################")
previous_count = get_latest_count(log_table, 'WeeklyIntegratedPartners')
display(previous_count)

#calculate minimum and maximum tolerance values (within 10%)
min_count_val, max_count_val = get_thresholds(previous_count['row_count'].values[0], 10)

# COMMAND ----------

# MAGIC %md ##Tests Begins

# COMMAND ----------

#test for current row count being within 10% of previous weeks count

info = "Checking that the row count is within the tolerance amount"
expect = df1.expect_table_row_count_to_be_between(min_value=min_count_val, max_value=max_count_val)
test_result(expect, info)
assert expect.success

# COMMAND ----------

#test that the sum of clicks column is within the tolerance amount

info = "Checking that the sum of the Clicks column is within the tolerance amount"
expect = df1.expect_column_sum_to_be_between(column='Clicks', min_value=min_sum_clicks, max_value=max_sum_clicks)
test_result(expect, info)
assert expect.success

# COMMAND ----------

#check there are 7 distinct date columns
info = 'Checking that there are 7 distinct dates in the date column'
expect = df1.expect_column_unique_value_count_to_be_between(column = 'Date', min_value = 7, max_value=7)
test_result(expect, info)
assert expect.success

# COMMAND ----------

#check that the count of distince ODS code has not changed more than 1% from previous week

info = "Checking that the count of distinct ODS codes is within the tolerance amount"
expect = df1.expect_column_unique_value_count_to_be_between(column='OdsCode', min_value=min_ods_count, max_value=max_ods_count)
test_result(expect, info)
assert expect.success

# COMMAND ----------

#check that the count of distince jumpoff has not changed at all since previous week

info = "Checking that the count of distinct jumpoffs has not changed since last week"
expect = df1.expect_column_unique_value_count_to_be_between(column='JumpOff', min_value=previous_jumpoff_count.iloc[0,3], max_value=previous_jumpoff_count.iloc[0,3])
test_result(expect, info)
#assert expect.success


# COMMAND ----------

#check that the count of distinct providers has not changed at all since previous week

info = "Checking that the count of distinct providers has not changed since last week"
expect = df1.expect_column_unique_value_count_to_be_between(column='Provider', min_value=previous_provider_count.iloc[0,3], max_value=previous_provider_count.iloc[0,3])
test_result(expect, info)
#assert expect.success

# COMMAND ----------

# MAGIC %md ##Tests End

# COMMAND ----------

# Count rows in file and write to log table
#___________________________________________
full_path = new_source_path + latestFolder + new_source_file
row_count = len(new_dataframe)
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
in_row = {'row_count':[row_count], 'load_date':[date], 'file_to_load':[full_path]}
df = pd.DataFrame(in_row)  
write_to_sql(df, log_table, "append")

# COMMAND ----------

# Write distinct ODS code count to the aggregate logg table
#___________________________________________________________
ods_count = len(new_dataframe['OdsCode'].unique()) #calculate the distinct ODS count from the current data

today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
count_row = {"load_date": [date], "file_name": [full_path], "aggregation": ["count"], "aggregate_value": [ods_count], "comment": ["count of unique ODS Codes"]}
agg_log_tbl = "dbo.pre_load_agg_log"
df_count = pd.DataFrame(count_row)  
write_to_sql(df_count, agg_log_tbl, "append")

# COMMAND ----------

# Write distinct jumpoffs count to the aggregate logg table
#___________________________________________________________
jumpoff_count = len(new_dataframe['JumpOff'].unique()) #calculate the distinct jumpoff count from the current data

today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
count_row = {"load_date": [date], "file_name": [full_path], "aggregation": ["count"], "aggregate_value": [jumpoff_count], "comment": ["count of unique Jump Offs"]}
agg_log_tbl = "dbo.pre_load_agg_log"
df_count = pd.DataFrame(count_row)  
write_to_sql(df_count, agg_log_tbl, "append")


# COMMAND ----------

# Write distinct jumpoffs count to the aggregate logg table
#___________________________________________________________
provider_count = len(new_dataframe['Provider'].unique()) #calculate the distinct provider count from the current data

today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
count_row = {"load_date": [date], "file_name": [full_path], "aggregation": ["count"], "aggregate_value": [provider_count], "comment": ["count of unique providers"]}
agg_log_tbl = "dbo.pre_load_agg_log"
df_count = pd.DataFrame(count_row)  
write_to_sql(df_count, agg_log_tbl, "append")

# COMMAND ----------

# Write sum to log tables
#___________________________________________
agg_row = {"load_date": [date], "file_name": [full_path], "aggregation": ["sum"], "aggregate_value": [sum_clicks], "comment": ["sum of the Clicks column"]}
agg_log_tbl = "dbo.pre_load_agg_log"
df_agg = pd.DataFrame(agg_row)  
write_to_sql(df_agg, agg_log_tbl, "append")
