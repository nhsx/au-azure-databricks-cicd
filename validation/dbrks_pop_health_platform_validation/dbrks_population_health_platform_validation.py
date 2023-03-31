# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_population_health_platform_validation.py
DESCRIPTION:
                Databricks notebook to validate Population health platform tracker data
USAGE:
                ...
CONTRIBUTORS:   Abdu Nuhu
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        28 March. 2022
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# ------------------------------------------------------------------------------------
%pip install pandas pathlib azure-storage-file-datalake numpy pyarrow==5.0.* great_expectations xlrd openpyxl

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

# Test
# -------------------------------------------------------------------------
file_path_config = "config/pipelines/direct-load"
file_name_config = "config_population_health_platform.json"
log_table = "dbo.pre_load_log"

file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON["pipeline"]['raw']["snapshot_source_path"]
new_source_file = "PHM_Platform.xlsx"


# COMMAND ----------

# Get latest folder and read file
# ----------------------------------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
print(new_source_path+latestFolder, new_source_file)
new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
new_dataframe = pd.read_excel(io.BytesIO(new_dataset), sheet_name = "Summary", header = 0, engine='openpyxl') 


# COMMAND ----------

# Date
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# Count rows in file
file_row_count = len(new_dataframe)
print("########### Number of rows in the file is shown below ###########################################")
print(file_row_count)


# COMMAND ----------

# validate data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
val_df = new_dataframe.mask(new_dataframe == " ") # convert all blanks to NaN for validtion
validation_df = ge.from_pandas(val_df) # Create great expectations dataframe from pandas datafarme


# COMMAND ----------

# Validate columns in file
#-----------------------------------------
info = "Checking that file has 42 row only"
expect = validation_df.expect_table_row_count_to_equal(value = 42)
test_result(expect, info)
assert expect.success



# COMMAND ----------

info = "Checking data types in file\n"
types = {
    "Region": "str",
    "ICS": "str",
    "PHM Platform": "str",
    "Survey": "str"
}
for column, type_ in types.items():
    print(column)
    expect = validation_df.expect_column_values_to_be_of_type(column=column, type_=type_)
    test_result(expect, info)
    assert expect.success

# COMMAND ----------

# Count rows in file and write to log table
#___________________________________________
full_path = new_source_path + latestFolder + new_source_file
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
in_row = {'row_count':[file_row_count], 'load_date':[date], 'file_to_load':[full_path]}
df = pd.DataFrame(in_row)  
write_to_sql(df, log_table, "append")
