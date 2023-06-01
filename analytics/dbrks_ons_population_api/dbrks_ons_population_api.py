# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_ons_population_api.py
DESCRIPTION:
                Databricks notebook with code to process raw ons population figures
USAGE:         
CONTRIBUTORS:   Oli Jones
CONTACT:        data@nhsx.nhs.uk
CREATED:        30 May 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* geopandas shapely geopandas shapely 

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

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/reference_tables/"
file_name_config = "config_ons_population_api.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']

sink_path = config_JSON['pipeline']['project']['sink_path']
sink_file = config_JSON['pipeline']['project']['sink_file ']

reference_source_path = config_JSON['pipeline']['project']['reference_source_path']
reference_source_file = config_JSON['pipeline']['project']['reference_source_file']

table_name = config_JSON['pipeline']["staging"][0]['sink_table']

# COMMAND ----------

#Processing
# source data
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)

# reference data
latestFolder_ref = datalake_latestFolder(CONNECTION_STRING, file_system, reference_source_path)
file_ref = datalake_download(CONNECTION_STRING, file_system, reference_source_path+latestFolder_ref, reference_source_file)

# Read in source data and reference file
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref = pd.read_parquet(io.BytesIO(file_ref), engine="pyarrow")

# Reduce columns in source file and filter for required dimensions
df = df[['DATE', 'GEOGRAPHY_CODE', 'C2021_AGE_92_NAME', 'C_SEX_NAME', 'OBS_VALUE']]
df = df.loc[(df['C_SEX_NAME'] == 'All persons') & (df['C2021_AGE_92_NAME'] != 'Total')]
df = df.groupby(by=['DATE', 'GEOGRAPHY_CODE']).agg({'OBS_VALUE':'sum'}).reset_index()
df = df.rename(columns={'GEOGRAPHY_CODE':'ICB_ONS_Code'})

# Reference processing
df_ref = df_ref[['ICB_ONS_Code', 'ICB_Code']]
df_ref = df_ref.drop_duplicates().reset_index(drop=True)

# Join
df2 = pd.merge(
  df, 
  df_ref, 
  how='left', 
  on=['ICB_ONS_Code']
)

df_processed = df2.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
