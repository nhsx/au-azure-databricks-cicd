# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_ndc_transactions_nhsapp_messaging_consultations_month_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric M236: OLC. Previously Messaging and Consultations. 
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli, Kabir Khan, Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        25th Aug 2022
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
source_file = config_JSON['pipeline']['project']["source_file_monthly"]
source_file_2 = config_JSON['pipeline']['project']["source_file_ods"]
sink_path = config_JSON['pipeline']['project']['databricks'][28]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][28]['sink_file'] 
table_name = config_JSON['pipeline']["staging"][28]['sink_table']

# COMMAND ----------

# Ingestion of numerator data (Monthly)
# ---------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# Ingestion of numerator data (Daily)
# ---------------------------------------------------------------------------------------------------
file_1 = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file_2)
df_econsult = pd.read_parquet(io.BytesIO(file_1), engine="pyarrow")

# COMMAND ----------

#Processing
# ---------------------------------------------------------------------------------------------------

#Numerator (Montly)
# ---------------------------------------------------------------------------------------------------
df_1 = df[["Monthly", "accuRx_messages", "Engage_admin", "Engage_medical", "accuRx_medical", "Engage_messages"]]
df_1.iloc[:, 0] = df_1.iloc[:,0].dt.strftime('%Y-%m')
df_2 = df_1.groupby(df_1.iloc[:,0]).sum().reset_index()

#Numerator (econuslt)
# ---------------------------------------------------------------------------------------------------
df_econsult_1 = df_econsult[["day", "count"]]
df_econsult_1.iloc[:, 0] = df_econsult_1.iloc[:,0].dt.strftime('%Y-%m')
df_econsult_2 = df_econsult_1.groupby(df_econsult_1.iloc[:,0]).sum().reset_index()

# Join Monthly and Daily Datasets
# ---------------------------------------------------------------------------------------------------
df_join = df_econsult_2.merge(df_2, how = 'left', left_on = 'day', right_on  = 'Monthly').drop(columns = 'Monthly')
col_list = ["accuRx_messages", "Engage_admin", "Engage_medical", "accuRx_medical", "Engage_messages", "count"]
df_join['OLCs on the NHS App'] = df_join[col_list].sum(axis=1)
df_join_1 = df_join.drop(columns = col_list)
df_join_1.rename(columns  = {'day': 'Date'}, inplace = True)
df_join_1.index.name = "Unique ID"
df_join_1['Date'] = pd.to_datetime(df_join_1['Date'])
df_processed = df_join_1.copy()

# COMMAND ----------

df_processed

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
