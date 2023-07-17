# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           ddbrks_ndc_messages_nhsapp_notifications_only.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric M274: Notifications Only
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli, Kabir Khan, Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        14th July 2023
VERSION:        0.0.3
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
file_name_config = "config_national_digital_channels_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file_messages']
sink_path = config_JSON['pipeline']['project']['databricks'][47]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][47]['sink_file']  
table_name = config_JSON['pipeline']["staging"][47]['sink_table']

# COMMAND ----------

# Ingestion of numerator data (Monthly)
# ---------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

#Processing
# ---------------------------------------------------------------------------------------------------

#Data Cleansing
# ---------------------------------------------------------------------------------------------------

#remove Internal and not mapped sender
df = df[~df['Supplier'].isin(['NHS App/Internal', 'Not Mapped'])]
#removed sendsers with 0 reads (assumed to be notification only)
df = df[df['Read By']==0]
df = df[['Date', 'Count']]
df = df.rename(columns = {'Count':'Notifications from Notification-Only Providers'})
df = df.groupby('Date').sum().reset_index()

df.index.name = "Unique ID"
df_processed = df.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder,sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
