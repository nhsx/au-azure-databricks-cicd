# Databricks notebook source
# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_cybersecurity_dspt_nhs_trusts_standards_compliance_by_ICB_validation.py
DESCRIPTION:    To reduce manual checks and improve quality validations need to added to dbrks_cybersecurity_dspt_nhs_trusts_standards_compliance_by_ICB pipeline for DSPT
USAGE:
CONTRIBUTORS:   Kabir Khan
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        4th Apr 2023
VERSION:        0.0.1
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
file_name_config = "config_dspt_nhs_dbrks.json"
log_table = "dbo.pre_load_log"

file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_path = config_JSON['pipeline']['project']['reference_path2']
reference_file = config_JSON['pipeline']['project']['reference_file2']
file_system =  dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
sink_path = config_JSON['pipeline']['project']['databricks'][2]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][2]['sink_file']
table_name = config_JSON['pipeline']["staging"][2]['sink_table']

# COMMAND ----------

# Processing 
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
reference_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
reference_file = datalake_download(CONNECTION_STRING, file_system, reference_path+reference_latestFolder, reference_file)
DSPT_df = pd.read_csv(io.BytesIO(file))
ODS_code_df = pd.read_parquet(io.BytesIO(reference_file), engine="pyarrow")

# COMMAND ----------

DSPT_df

# COMMAND ----------

ODS_code_df

# COMMAND ----------

# Validate data for DSPT data and ODS code
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
val_df = DSPT_df.mask(DSPT_df == " ") # convert all blanks to NaN for validtion
val_df_2 = ODS_code_df.mask(ODS_code_df == " ") # convert all blanks to NaN for validtion
df1 = ge.from_pandas(val_df) # Create great expectations dataframe from pandas datafarme
df2 = ge.from_pandas(val_df_2) # Create great expectations dataframe from pandas datafarme
print("############################## Content of the file under test ############################################") 
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests Begin

# COMMAND ----------

# Test that if there's all value in the Code column are UNIQUE
expect = df1.expect_column_values_to_be_unique(column="Code")
assert expect.success

# Test that if there's all value in the Organisation_Code column are UNIQUE
expect = df2.expect_column_values_to_be_unique(column="Organisation_Code")
assert expect.success


# COMMAND ----------

# Test that these columns value are not NULL for DSPT_df
for column in ["Code", "Organisation Name", "Status", "Primary Sector"]:
    expect = df1.expect_column_values_to_not_be_null(column=column)
    assert expect.success

# Test that columns value are not NULL for ODS_Code_df
for column in ["Organisation_Code", "Organisation_Name", "Region_Code", "Region_Name", "STP_Code", "STP_Name", "ODS_Organisation_Type"]:
    expect = df2.expect_column_values_to_not_be_null(column=column)
    assert expect.success

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests End

# COMMAND ----------

# Count rows in file and write to log table
#___________________________________________
full_path = source_path + latestFolder + source_file
row_count = len(DSPT_df)
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
in_row = {'row_count':[row_count], 'load_date':[date], 'file_to_load':[full_path]}
df = pd.DataFrame(in_row)  
write_to_sql(df, log_table, "append")

# COMMAND ----------

# Count rows in file and write to log table
#___________________________________________
full_path = reference_path + reference_latestFolder + reference_file
row_count = len(ODS_code_df)
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
in_row = {'row_count':[row_count], 'load_date':[date], 'file_to_load':[full_path]}
df = pd.DataFrame(in_row)  
write_to_sql(df, log_table, "append")
