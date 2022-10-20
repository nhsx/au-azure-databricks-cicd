# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_nhs_app_jumpoff_pkb_healthTrackers_day_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric M0170: PKB - healthTrackersJump Off Clicks
USAGE:
                ...
CONTRIBUTORS:   Shahriar Kabir Khan
CONTACT:        data@nhsx.nhs.uk
CREATED:        8th Aug 2022
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.*

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

#Download JSON config from Azure datalake
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_nhs_app_jumpoff_dbrks.json" 
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][12]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][12]['sink_file']
table_name = config_JSON['pipeline']['staging'][12]['sink_table']

# COMMAND ----------

# Ingestion of numerator data (NHS app performance data)
# ---------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

#Processing
# ---------------------------------------------------------------------------------------------------
df1 = df[["Date", "OdsCode","Provider", "JumpOff","Clicks"]].copy()
df1["Clicks"] = df1["Clicks"].astype(int)
df1 = df1[((df1['JumpOff']=='healthTrackersCie')| (df1['JumpOff']=='healthTrackers'))  & (df1['Provider']=='Patients Know Best')]
df1 = df1.groupby(['Provider','Date','OdsCode'], as_index=False).sum()
df1['Date'] = pd.to_datetime(df1['Date'], infer_datetime_format=True)
df2 = df1[df1['Date'] >= '2021-01-01'].reset_index(drop = True)  #--------- remove rows pre 2021
df3 = df2[['Provider','Date', 'OdsCode', 'Clicks']]
df4 = df3.rename(columns = {'OdsCode': 'Practice code', 'Clicks': 'Number of Health Trackers Jump Off Clicks'})
df4.index.name = "Unique ID"
df_processed = df4.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
