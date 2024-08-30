# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_nhs_app_monthlyDownload_validation.py
                ...
CONTRIBUTORS:   Abdu Nuhu
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        17 Jan. 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# ------------------------------------------------------------------------------------
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
file_name_config = "config_nhs_app_monthly_device_dbrks.json"
log_table = "dbo.pre_load_log"
agg_log_tbl = "dbo.pre_load_agg_log"

file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON["pipeline"]['raw']["snapshot_source_path"]

# COMMAND ----------

# Get latest folder and read file
# ----------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
file_name_list = [file for file in file_name_list if "MonthlyDownloads" in file] 
if not file_name_list:
    print("MonthlyDownloads file has not arrived")
    dbutils.notebook.exit("MonthlyDownloads file has not arrived")

for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset))

# COMMAND ----------

# Get today's count and sum
# ------------------------------------------------
today_count = len(new_dataframe)
sum_clicks = new_dataframe["Count"].sum()
print("Today's row count is: " + str(today_count))
print("Today's row sum is: " + str(sum_clicks))


# COMMAND ----------

# Get the sum of the count column from the previous weeks data and calculate the 20% thresholds
# ---------------------------------------------------------------------------------------------
last_run = get_last_agg(agg_log_tbl, "MonthlyDownloads", "sum", "sum of the count column")
print("############# Last run details is shown below ###############################")
display(last_run)

previous_sum = last_run.iloc[0,3]

#calculate thresholds using function from helper functions
min_sum_clicks, max_sum_clicks = get_thresholds(previous_sum, 20)

# COMMAND ----------

#get the number of days in the current month and calculate the number of days
month_df = new_dataframe.copy()
month_df['Date'] = pd.to_datetime(month_df['Date']) #convert the Date column to datetime to get the number of days
number_of_days = month_df['Date'][0].daysinmonth


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

info = "Checking that the sum of downloads which is the count column is within the tolerance amount"
expect = df1.expect_column_sum_to_be_between(column='Count', min_value=min_sum_clicks, max_value=max_sum_clicks)
test_result(expect, info)
assert expect.success


# COMMAND ----------

info = 'Checking that all dates are unique \n'
expect = df1.expect_column_values_to_be_unique(column="Date")
test_result(expect, info)
assert expect.success


# COMMAND ----------

info = 'Checking that row count matches number of days in that month\n'
expect = df1.expect_table_row_count_to_equal(number_of_days)
test_result(expect, info)
assert expect.success

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests End

# COMMAND ----------

# Get todays date
#------------------------------------------------------
today = pd.to_datetime("now").strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, "%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# Write row count to log tables
#___________________________________________
full_path = new_source_path + latestFolder + new_source_file
row_count = len(new_dataframe)
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
in_row = {"row_count":[today_count], "load_date":[date], "file_to_load":[full_path]}
df = pd.DataFrame(in_row)  
write_to_sql(df, log_table, "append")



# COMMAND ----------

# Write sum to log tables
#___________________________________________

agg_row = {"load_date": [date], "file_name": [full_path], "aggregation": ["sum"], "aggregate_value": [sum_clicks], "comment": ["sum of the count column"]}
agg_log_tbl = "dbo.pre_load_agg_log"
df_agg = pd.DataFrame(agg_row)  
write_to_sql(df_agg, agg_log_tbl, "append")
