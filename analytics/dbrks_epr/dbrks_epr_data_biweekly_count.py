# Databricks notebook source
# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:          dbrks_epr_data_biweekly_count.py
DESCRIPTION:
                Databricks notebook with processing code for DCT Metrics: FILE: Epr Data Biweekly Count (M390a)
(M387)
USAGE:
                ...
CONTRIBUTORS:   Everistus Oputa
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        11 May 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==8.0.*

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
file_name_config = "config_epr_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_path = config_JSON['pipeline']['project']['denominator_source_path']
reference_file = config_JSON['pipeline']['project']['denominator_source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][0]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][0]['sink_file']
table_name = config_JSON['pipeline']['staging'][0]['sink_table'] 

# COMMAND ----------

#Denominator data ingestion and processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system,reference_path+latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref = df_ref.rename(columns = {'Organisation_Code':'ODS'})
df_ref = df_ref[['ODS', 'STP_Code','Organisation_Name']]

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df1 = df[["Organisation Code", "Trust Type", "EPR Group","Current EPR Status","BiWeekly_Date","Current Forecast Go-Live"]]
df1 = df1.rename(columns = {'Organisation Code':'ODS','Trust Type':'Trust_Type','EPR Group':'EPR_Group','Current EPR Status':'Current_EPR_Status','BiWeekly_Date':'Bi_Weekly_Report_Date','Current Forecast Go-Live':'Current_Forecast_Go_Live'})
# df1 = df.copy()
# df1['Date'] = pd.to_datetime(df1['Bi_Weekly_Report_Date'])
df1.index.name = "Unique ID"
df_processed = df1.copy()

list_of_ODS = df_processed['ODS']
new_ODS_list = []
for i in list_of_ODS:
  if '\xa0' in i:
    new_ODS_list.append(i.replace('\xa0', ''))
  else:
    new_ODS_list.append(i)

df_processed['ODS'] = new_ODS_list



# COMMAND ----------

df_processed = df_processed.merge(df_ref, on = 'ODS', how = 'left')
df_processed

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
