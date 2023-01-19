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
CONTRIBUTORS:   Abdu Nuhu, Kabir Khan
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        16 Jan 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------------------
%pip install pandas pathlib azure-storage-file-datalake numpy pyarrow==5.0.* great_expectations openpyxl 

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
file_name_config = "config_nhs_app_dbrks.json"
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
file_name_list = [file for file in file_name_list if 'nhs_app_table_snapshot' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset))
  new_dataframe['Date'] = pd.to_datetime(new_dataframe['Date']).dt.strftime("%Y-%m-%d")

# COMMAND ----------

#store the sum of each column for the new dataframe in a dictionary
sum_of_current_cols = {}
for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  sum_of_current_cols[column] = new_dataframe[column].sum()
sum_of_current_cols

# COMMAND ----------

new_dataframe

# COMMAND ----------

#load in the sums for the previous dataset
df_sum_prev = pd.DataFrame(columns=['load_date', 'file_name', 'aggregation', 'aggregate_value', 'comment'])
for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  prev_agg_row = get_last_agg(agg_log_table, 'app_table_snapshot', "sum_of_" + column, "sum of {} column".format(column))
  df_sum_prev = df_sum_prev.append(prev_agg_row)
df_sum_prev

# COMMAND ----------

df_sum_prev.loc[df_sum_prev['aggregation'].str.contains('P5NewAppUsers')]['aggregate_value'].values[0]

# COMMAND ----------

sum_of_current_cols['P5NewAppUsers']

# COMMAND ----------

#get the row count for previous data and calculate minimum and maximum tolerance values (within 10%)
print("############# Last run details is shown below ###############################")
display(previous_df)

thresholds_dict = {}

for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  todays_sum = int(sum_of_current_cols[column])
  previous_sum = int(df_sum_prev.loc[df_sum_prev['aggregation'].str.contains(column)]['aggregate_value'].values[0])
  percent = 20 / 100
  tolerance = round(percent * previous_sum)
  min_val = previous_sum - tolerance
  max_val = todays_sum + tolerance
  min_max = [min_val, max_val]
  thresholds_dict[column] = min_max

thresholds_dict




# COMMAND ----------

# Get today's count and sum
# ------------------------------------------------
todays_count = len(new_dataframe) - 1                        # subtracting 1 as we added total of the column at the end of the df

# Get previous count and sum

# ------------------------------------------------
previous_df = get_latest_count(log_table, new_source_file)
previous_count = previous_df.iloc[0, 0]

# COMMAND ----------


#get the row count for previous data and calculate minimum and maximum tolerance values (within 10%)
print("############# Last run details is shown below ###############################")
display(previous_df)

percent = 10 / 100
tolerance = round(percent * previous_count)
min_val = previous_count - tolerance
max_val = todays_count + tolerance

print("Percentage is :")
print(percent)
print("Previous count is :")
print(previous_count)
print("Tolerence is :")
print(tolerance)
print("Minimum expected sum is :")
print(min_val)
print("Maximum expected sum is :")
print(max_val)

# COMMAND ----------


#get the distince ODS count for previous data and calculate the minimum and maximum tolerance values (within 1%)

# COMMAND ----------

# validate data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
val_df = new_dataframe.mask(new_dataframe == " ") # convert all blanks to NaN for validtion
df1 = ge.from_pandas(val_df) # Create great expectations dataframe from pandas datafarme

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests Begin

# COMMAND ----------

info = "Checking that the row count is within the tolerance amount"
expect = df1.expect_table_row_count_to_be_between(min_value=min_val, max_value=max_val)
test_result(expect, info)
assert expect.success

# COMMAND ----------

new_dataframe.columns

# COMMAND ----------

#Test that the all the column have expected type such as int, str etc
#     "P5NewAppUsers": "int",                #this column have null value
#     "AcceptedTermsAndConditions": "int",   #this column have null value
#     "P9VerifiedNHSAppUsers": "int",        #this column have null value
info = "Checking that all the columns have type integer\n"
types = {
    "P5NewAppUsers": "float",
    "Logins": "int",
    "RecordViews": "int",
    "Prescriptions": "int",
    "NomPharmacy": "int",
    "AppointmentsBooked": "int",
    "AppointmentsCancelled": "int",
    "ODRegistrations": "int",
    "ODWithdrawals": "int",
    "ODUpdates": "int",
    "ODLookups": "int",
    "RecordViewsDCR": "int",
    "RecordViewsSCR": "int"
}
for column, type_ in types.items():
    expect = df1.expect_column_values_to_be_of_type(column=column, type_=type_)
    test_result(expect, info)
    assert expect.success
    

# COMMAND ----------

#Test that the OdsCode column do not contain any null values

info = "Checking that the OdsCode column do not contain any null values\n"
expect = df1.expect_column_values_to_not_be_null(column='OdsCode')
test_result(expect, info)
assert expect.success


# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests End

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

# Write sum to log tables
#___________________________________________

df_all_agg = pd.DataFrame(columns=['load_date', 'file_name', 'aggregation', 'aggregate_value', 'comment'])

for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  sum_value = int(new_dataframe[column].sum())
  today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
  date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
  agg_row = {"load_date": [date], "file_name":[full_path], "aggregation":["sum_of_" + column], "aggregate_value":[sum_value], "comment":["sum of {} column".format(column)]}
  agg_log_tbl = "dbo.pre_load_agg_log"
  df_agg = pd.DataFrame(agg_row)  
  df_all_agg = df_all_agg.append(df_agg)

write_to_sql(df_all_agg, agg_log_tbl, "append")

# COMMAND ----------

df_all_agg

# COMMAND ----------

# Write sum to log tables
#___________________________________________
ods_count = len(new_dataframe['OdsCode'].unique())

count_row = {"load_date": [date], "file_name": [full_path], "column_name": ["OdsCode"], "count_value": [ods_count]}
count_log_tbl = "dbo.pre_load_count_log"
df_count = pd.DataFrame(count_row)  
write_to_sql(df_count, count_log_tbl, "append")

# COMMAND ----------


