# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_toc_messages_provider_sent_week_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: No. transfer of care digital messages sent to GPs at a NHS Trust level (all use cases) (M030A.1)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        23 Aug 2022
VERSION:        0.0.2
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

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_toc_messages_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][2]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][2]['sink_file']
table_name = config_JSON['pipeline']["staging"][2]['sink_table']

# COMMAND ----------

#Processing
#------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df1 = df[['_time', 'workflow','recipientOdsCode']]
df1 = df1[df1['workflow'].str.contains('ACK')].reset_index(drop = True)
df1['Count'] = 1
df1['_time'] = pd.to_datetime(df1['_time']).dt.strftime("%Y-%m")
df2 = df1.groupby(['_time', 'workflow', 'recipientOdsCode']).sum().reset_index()
df2['Count'] = df2['Count'].div(2).apply(np.floor)
df3 = df2.set_index(['_time','recipientOdsCode','workflow']).unstack()['Count'].reset_index().fillna(0)
df4 = df3.rename(columns = {'_time': 'Date', 
                            'recipientOdsCode': 'Trust code', 
                            'TOC_FHIR_EC_DISCH_ACK': 'Number of successful FHIR ToC emergency care discharge messages',
                            'TOC_FHIR_IP_DISCH_ACK': 'Number of successful FHIR ToC acute admitted patient care discharge messages',
                            'TOC_FHIR_MH_DISCH_ACK': 'Number of successful FHIR ToC mental health discharge messages',
                            'TOC_FHIR_OP_ATTEN_ACK': 'Number of successful FHIR ToC outpatient clinic attendance messages'})
df4['Trust code'] = df4['Trust code'].str[:3] #------ Only retain the first three characters of the NHS Trust Site ODS code, to equate it to the NHS Trust ODS code
df4.columns.name = None
if df4['Date'].iloc[-1] == datetime.now().strftime("%Y-%m"):
  df4 = df4[(df4['Date'] < datetime.now().strftime("%Y-%m"))]
df4.index.name = "Unique ID"
df4["Date"] = pd.to_datetime(df4["Date"])
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
