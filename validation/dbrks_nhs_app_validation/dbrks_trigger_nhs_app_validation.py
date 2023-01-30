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

#get the totals for each column and store the totals in a dataframe
sum_of_current_cols = {'column_name':list(new_dataframe.columns)[2:len(new_dataframe.columns)]}
list_of_sums = []
for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  list_of_sums.append(new_dataframe[column].sum())

sum_of_current_cols['column_sum'] = list_of_sums

#print the dataframe of totals for the current data
df_sum_of_current_cols = pd.DataFrame(sum_of_current_cols)
print('############################# - SUMS FOR EACH COLUMN FROM CURRENT RUN SHOWN BELOW ###############################')
df_sum_of_current_cols


# COMMAND ----------

#load in the sums of each column for the previous dataset and store this in a dataframe
df_sum_prev = pd.DataFrame(columns=['load_date', 'file_name', 'aggregation', 'aggregate_value', 'comment'])
for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  prev_agg_row = get_last_agg(agg_log_table, 'app_table_snapshot', "sum", "sum of {} column".format(column))
  df_sum_prev = df_sum_prev.append(prev_agg_row)


# COMMAND ----------

#check the dataframe to see sums from previous year are loaded in correctly by displaying the totals, date stamp and file paths
print('############################# - SUMS FOR EACH COLUMN FROM PREVIOUS RUN SHOWN BELOW ###############################')
df_sum_prev

# COMMAND ----------

#initialise dictionary to store the minimum and maximum values to compare the current data totals with
thresholds_dict = {}

#loop through each column calculate thresholds with 20% tolerance and store in the dictionary
#thresholds calculated using the get_thresholds function from helper functions
for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  print("The threshold for {} is calculated as follows".format(column))
  previous_sum = df_sum_prev.loc[df_sum_prev['comment'].str.contains(column)]['aggregate_value'].values[0] #get the previous sum for each column from the dataframe above^
  min_val, max_val = get_thresholds(previous_sum, 20)
  min_max = [min_val, max_val]
  thresholds_dict[column] = min_max
  print()



# COMMAND ----------

#get the row count for last weeks data and display the information
print("############# Last run details is shown below ###############################")
previous_count = get_latest_count(log_table, 'app_table_snapshot')
display(previous_count)

#calculate minimum and maximum tolerance values (within 10%)
min_count_val, max_count_val = get_thresholds(previous_count['row_count'].values[0], 10)

# COMMAND ----------

#get the unique ODS count from the previous week and calculate the minimum and maximum thresholds
print("############# Last run details is shown below ###############################")
previous_ods_count = get_last_agg(agg_log_table, 'app_table_snapshot', "count", "count of unique ODS Codes")
display(previous_ods_count)

#calculate minimum and maximum tolerance values (within 1%)
min_ods_count, max_ods_count = get_thresholds(previous_ods_count['aggregate_value'].values[0], 10)

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

#test for current row count being within 10% of previous weeks count

info = "Checking that the row count is within the tolerance amount"
expect = df1.expect_table_row_count_to_be_between(min_value=min_count_val, max_value=max_count_val)
test_result(expect, info)
assert expect.success

# COMMAND ----------

#test that the sum of each column is within the tolerance amount from the previous week

for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  info = "Checking that the sum of {} is within the tolerance amount".format(column)
  expect = df1.expect_column_sum_to_be_between(column=column, min_value=thresholds_dict[column][0], max_value=thresholds_dict[column][1])
  test_result(expect, info)
  assert expect.success

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

#check there are 7 distinct date columns
info = 'Checking that there are 7 distinct dates in the date column'
expect = df1.expect_column_unique_value_count_to_be_between(column = 'Date', min_value = 7, max_value=7)
test_result(expect, info)
assert expect.success

# COMMAND ----------

#Test that the OdsCode column do not contain any null values

info = "Checking that the OdsCode column do not contain any null values\n"
expect = df1.expect_column_values_to_not_be_null(column='OdsCode')
test_result(expect, info)
assert expect.success


# COMMAND ----------

#check that the count of distince ODS code has not changed more than 1% from previous week

info = "Checking that the count of distinct ODS codes is within the tolerance amount"
expect = df1.expect_column_unique_value_count_to_be_between(column='OdsCode', min_value=min_ods_count, max_value=max_ods_count)
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

# Write the sum of each column to the aggregate log tables
#__________________________________________________________

df_all_agg = pd.DataFrame(columns=['load_date', 'file_name', 'aggregation', 'aggregate_value', 'comment'])
#loop through each column and calculate the total

for column in new_dataframe.columns[2:len(new_dataframe.columns)]:
  sum_value = int(new_dataframe[column].sum())
  full_path = new_source_path + latestFolder + new_source_file
  today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
  date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
  agg_row = {"load_date": [date], "file_name":[full_path], "aggregation":["sum"], "aggregate_value":[sum_value], "comment":["sum of {} column".format(column)]}
  agg_log_tbl = "dbo.pre_load_agg_log"
  df_agg = pd.DataFrame(agg_row)  
  df_all_agg = df_all_agg.append(df_agg)
  
#write the total to the aggregate log table with the latest time stamp
write_to_sql(df_all_agg, agg_log_tbl, "append")

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


