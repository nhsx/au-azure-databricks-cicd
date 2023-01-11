# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_trigger_socialcare_digitalrecord_validation.py
DESCRIPTION:
                To reduce manual checks and improve quality validations need to added to trigger_socialcare_digitalrecord pipeline for digital social care
USAGE:
                ...
CONTRIBUTORS:   Abdu Nuhu, Kabir Khan, Faaiz Shanawas
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        14 Dec. 2022
VERSION:        0.0.3
"""

# COMMAND ----------

# Install libs
# ------------------------------------------------------------------------------------
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
file_name_config = "config_digitalrecords_socialcare_dbrks.json"
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

# Get latest folder and read file
# ----------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
file_name_list = [file for file in file_name_list if '.xlsx' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_excel(io.BytesIO(new_dataset), sheet_name='PIR responses', engine  = 'openpyxl')

# COMMAND ----------

# validate data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
val_df = new_dataframe.mask(new_dataframe == " ") # convert all blanks to NaN for validtion
df1 = ge.from_pandas(val_df) # Create great expectations dataframe from pandas datafarme

# drop rows which have same 'Location ID' and 'Use a Digital Social Care Record system?' and keep latest entry
validation_df = df1.drop_duplicates(subset = ['Location ID', 'Use a Digital Social Care Record system?'],keep = 'last').reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests Begin

# COMMAND ----------

#Test that location bed capacity data type are all int

types = {
    "Location bed capacity": "int"
}
info = "Checking that 'Location bed capacity' column data are all int\n"
for column, type_ in types.items():
    expect = validation_df.expect_column_values_to_be_of_type(column=column, type_=type_)
    assert expect.success
    test_result(expect, info)

# COMMAND ----------

#Test that the location ID column contains no nulls

info = 'Checking that Location ID has no Nulls\n'
expect = validation_df.expect_column_values_to_not_be_null("Location ID") # Check that has no null or blank values
test_result(expect, info)
assert expect.success


# COMMAND ----------

#Check for only Yes or No values in the digital social care record system column

info = 'Checking that Use a Digital Social Care Record system? only has YES and NO\n'
expect = validation_df.expect_column_values_to_be_in_set(column="Use a Digital Social Care Record system?", value_set=["Yes", "No", "yes", "no"]) # Check that Dormant (Y/N) column only has "Y", "N", "y", "n"
test_result(expect, info)
assert expect.success


# COMMAND ----------

#Check that the PIR type only contains the correct specified values in the value set

info = 'Checking that PIR type only has Residential, Community, Shared Lives\n'
expect = validation_df.expect_column_values_to_be_in_set(
    column="PIR type",
    value_set=["Residential", "Community", "Shared Lives"],
)
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
