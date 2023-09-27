# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_home_care_user_service_count_validation.py
DESCRIPTION:
                To reduce manual checks and improve quality validations need to added to dbrks_home_care_user_service_count_validation pipeline for digital social care
USAGE:
                ...
CONTRIBUTORS:   Kabir Khan
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        28 Feb 2023
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
file_name_config = "config_home_care_user_service.json"
log_table = "dbo.pre_load_log"

file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON["pipeline"]['raw']["snapshot_source_path"]

# COMMAND ----------

# Pull new domiciliary  dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+ latestFolder)
file_name_list = [file for file in file_name_list if 'Home Care' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  df_dom = pd.read_excel(io.BytesIO(new_dataset), header = 0, engine='openpyxl')

# COMMAND ----------

#Pull new care home dataset
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+ latestFolder)
care_home_list = [file for file in file_name_list if 'Care Home' in file]
for care_home_file in care_home_list:
  care_home_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, care_home_file)
  df_res = pd.read_excel(io.BytesIO(care_home_dataset), sheet_name = 'CH residents', header = 0, engine='openpyxl')

# COMMAND ----------

# validate dom data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------}
val_df_dom = df_dom.mask(df_dom == " ") # convert all blanks to NaN for validtion
df_dom1 = ge.from_pandas(val_df_dom) # Create great expectations dataframe from pandas datafarme

# COMMAND ----------

# validate res data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------}
val_df_res = df_res.mask(df_res == " ") # convert all blanks to NaN for validtion
df_res1 = ge.from_pandas(val_df_res) # Create great expectations dataframe from pandas datafarme

# COMMAND ----------

df_res['LastUpdatedBst'][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests Begin

# COMMAND ----------

#Test that the CqcID column do not contain any null values in the domiciliary data

info = "Checking that the CqcId column do not contain any null values in domiciliary data\n"
expect = df_dom1.expect_column_values_to_not_be_null(column='CqcId')
test_result(expect, info)
assert expect.success

# COMMAND ----------

#Test that the CqcId column do not contain any null values in the residential data

info = "Checking that the CqcId column do not contain any null values in residential data\n"
expect = df_res1.expect_column_values_to_not_be_null(column='CqcId')
test_result(expect, info)
assert expect.success

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests End

# COMMAND ----------

# Count rows in file and write to log table for domiciliary data
#___________________________________________
full_path = new_source_path + latestFolder + new_source_file
row_count = len(df_dom)
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
in_row = {'row_count':[row_count], 'load_date':[date], 'file_to_load':[full_path]}
df = pd.DataFrame(in_row)  
write_to_sql(df, log_table, "append")

# COMMAND ----------

# Count rows in file and write to log table for domiciliary data
#___________________________________________
full_path = new_source_path + latestFolder + care_home_file
row_count = len(df_res)
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
in_row = {'row_count':[row_count], 'load_date':[date], 'file_to_load':[full_path]}
df = pd.DataFrame(in_row)  
write_to_sql(df, log_table, "append")
