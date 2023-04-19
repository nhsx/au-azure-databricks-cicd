# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_count_rows_in_table.py
DESCRIPTION:
                Databricks notebook to count rows SQL in a table
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
%pip install pandas pathlib azure-storage-file-datalake numpy pyarrow==5.0.*

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

# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Shared/databricks/au-azure-databricks-cicd/functions/dbrks_helper_functions

# COMMAND ----------

# Load parameters and JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = dbutils.widgets.get("adf_file_path")
file_name_config = dbutils.widgets.get("adf_file_name")
log_table = dbutils.widgets.get("adf_log_table")

print('------------ Configuration -------------------')

print(file_path_config)
print(file_name_config)
print(log_table)

print('-----------------------------------------------')

file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
staging = config_JSON['pipeline']["staging"]

print('----------------- Tables from config ---------------')
print(staging)
print('-----------------------------------------------------')


# COMMAND ----------

# Read and aggregate table data
# -------------------------------------------------------------------------
today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
staging_tbl = ''

for entry in staging:
  for key in entry:
    if key == 'sink_table':
      print('----------- Table to count --------------')
      print(entry[key])
      print('------------------------------------------')
      staging_tbl = entry[key]
      spark_df = read_sql_server_table(staging_tbl)
      row_count = spark_df.count()
      in_row = {'load_date':[date], 'tbl_name':[staging_tbl], 'aggregation':'Count', 'aggregate_value':[row_count]}
      print('----------- Record to write in table --------------')
      print(in_row)
      print('----------------------------------------------------')
      df = pd.DataFrame(in_row)
      write_to_sql(df, log_table, "append")
        
