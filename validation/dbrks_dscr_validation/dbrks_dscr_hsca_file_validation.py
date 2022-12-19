# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dscr_haca_file_validation.py
DESCRIPTION:
                Databricks notebook to validate HSCA_Active_Locations file dowmload from CQC website
USAGE:
                ...
CONTRIBUTORS:   Abdu Nuhu
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        07 Nov. 2022
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# ------------------------------------------------------------------------------------
%pip install pandas pathlib azure-storage-file-datalake numpy pyarrow==5.0.* great_expectations

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
file_path_config = "config/pipelines/nhsx-au-analytics"
file_name_config = "config_dscr_dbrks.json"
log_table = "dbo.pre_load_log"

file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON["pipeline"]['raw']["snapshot_source_path"]
new_source_file = "HSCA_Active_Locations .csv"

# COMMAND ----------

# Get latest folder and read file
# ----------------------------------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
print(new_source_path+latestFolder, new_source_file)
new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
new_dataframe = pd.read_csv(io.BytesIO(new_dataset), encoding = "ISO-8859-1")

# COMMAND ----------

# validate data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
val_df = new_dataframe.mask(new_dataframe == " ") # convert all blanks to NaN for validtion
validation_df = ge.from_pandas(val_df) # Create great expectations dataframe from pandas datafarme

# Validate columns in file
#-----------------------------------------
expect = validation_df.expect_column_values_to_not_be_null("Location ODS Code") # Check that has no null or blank values
assert expect.success

expect = validation_df.expect_column_values_to_be_in_set(column="Dormant (Y/N)", value_set=["Y", "N", "y", "n"]) # Check that Dormant (Y/N) column only has "Y", "N", "y", "n"
assert expect.success

expect = validation_df.expect_column_values_to_be_in_set(column="Care home?", value_set=["Y", "N", "y", "n"])  # Check that Care home? column only has "Y", "N", "y", "n"
assert expect.success


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

# The columns needed from the file
# -------------------------------------
#["Location ID", "Dormant (Y/N)","Care home?","Location Inspection Directorate","Location Primary Inspection Category","Location ONSPD CCG Code","Provider ID","Provider Inspection Directorate","Provider Primary Inspection Category","Provider Postal Code","Active"]

