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
agg_log_table = 'dbo.pre_load_agg_log'

file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON["pipeline"]['raw']["snapshot_source_path"]
new_source_file = "HSCA_Active_Locations.csv"

# COMMAND ----------

# Get latest folder and read file
# ----------------------------------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
print(new_source_path+latestFolder, new_source_file)
new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
new_dataframe = pd.read_csv(io.BytesIO(new_dataset), encoding = "ISO-8859-1")


# COMMAND ----------

# Date
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# Count today's Location and Location Inspection Directorate
location_count = new_dataframe["Location ID"].count()
print("########### Number of Location is Shown Below ###########################################")
print(location_count)


# COMMAND ----------

# validate data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
val_df = new_dataframe.mask(new_dataframe == " ") # convert all blanks to NaN for validtion
validation_df = ge.from_pandas(val_df) # Create great expectations dataframe from pandas datafarme


# COMMAND ----------

# Get the previous Count for Location, if there is no previous count log today's count
location_agg = get_last_agg(agg_log_table, new_source_file, "Count", "Count of Location ID for CQC file")
print(new_source_file)
if not location_agg.empty:
  print("############# Last run details is shown below #############################################")
  display(location_agg)
  
  location_previous_count = location_agg['aggregate_value'].values[0] 
  
  print("############# Location ID previous count is shown below ######################################")
  print(location_previous_count)
  print("##############################################################################################")
  
  location_min, location_max = get_thresholds(location_previous_count, 5)
  
  # Test
  info = "Checking that the row count for Location ID is within the tolerance amount"
  expect = validation_df.expect_table_row_count_to_be_between(min_value=location_min, max_value=location_max)
  test_result(expect, info)
  assert expect.success
else:
  print("############# No previous run found, this is taken to be the first ever run for this #######")


# COMMAND ----------

# Validating Location Inspection Directorate column with Adult social care in CQC file is about 27,100 plus or minus 10%
# ------------------------------------------------------------------------------------------------------------------------------------
loc_inspection = pd.DataFrame(new_dataframe["Location Inspection Directorate"].str.upper())
location_insp = loc_inspection[loc_inspection["Location Inspection Directorate"] == "ADULT SOCIAL CARE"]
adult_socialcare_count = len(location_insp)

print("############### Today's count of Location Inspection Directorate column with Adult social care is shown below ###")
print(adult_socialcare_count)
print("#################################################################################################################")

loc_insp_ge_df = ge.from_pandas(location_insp)
min_no, max_no = get_thresholds(27100, 10)

info = "Checking that Inspection Directorate column with Adult social care row count is about 27,100 plus-minus 10 percent"
expect = loc_insp_ge_df.expect_table_row_count_to_be_between(min_value=min_no, max_value=max_no)
assert expect.success


# COMMAND ----------

# Validate columns in file
#-----------------------------------------
info = "Checking that Location ID column does not have NULLs"
expect = validation_df.expect_column_values_to_not_be_null("Location ID") # Check that has no null or blank values
test_result(expect, info)
assert expect.success

info = "Checking that Dormant (Y/N) column has only Y and N"
expect = validation_df.expect_column_values_to_be_in_set(column="Dormant (Y/N)", value_set=["Y", "N", "y", "n"]) # Check that Dormant (Y/N) column only has "Y", "N", "y", "n"
test_result(expect, info)
assert expect.success

info = "Checking that Care home column has only Y and N"
expect = validation_df.expect_column_values_to_be_in_set(column="Care home?", value_set=["Y", "N", "y", "n"])  # Check that Care home? column only has "Y", "N", "y", "n"
test_result(expect, info)
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

# Log Location ID row count for today's file
cqc_full_path = new_source_path + new_source_file
location_agg_row = {"load_date": [date], "file_name": [cqc_full_path], "aggregation": ["Count"], "aggregate_value": [location_count], "comment": ["Count of Location ID for CQC file"]}
location_df = pd.DataFrame(location_agg_row) 
print("############# Log the follow to dbo.pre_load_agg_log table ######################################")
display(location_df)
write_to_sql(location_df, agg_log_table, "append")

# COMMAND ----------

# Log Location Inspection Directorate column with Adult social care row count for today's file
cqc_full_path = new_source_path + new_source_file
location_adult_socialcare_agg_row = {"load_date": [date], "file_name": [cqc_full_path], "aggregation": ["Count"], "aggregate_value": [adult_socialcare_count], "comment": ["Count of Inspection Directorate column with Adult social care row count"]}
location_adult_socialcare_df = pd.DataFrame(location_adult_socialcare_agg_row) 
print("############# Log the follow to dbo.pre_load_agg_log table ######################################")
display(location_adult_socialcare_df)
write_to_sql(location_adult_socialcare_df, agg_log_table, "append")
