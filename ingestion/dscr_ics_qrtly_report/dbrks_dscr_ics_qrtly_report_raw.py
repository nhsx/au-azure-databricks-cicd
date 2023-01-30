# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dscr_ics_qrtly_report_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical for digital social care quarterly ICS report
              
USAGE:
                ...
CONTRIBUTORS:   Abdu Nuhu
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        26 Jan. 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* xlrd openpyxl python-dateutil fastparquet

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
import json
import fastparquet

# 3rd party:
import pandas as pd
import numpy as np
import requests
from pathlib import Path
from urllib import request as urlreq
from bs4 import BeautifulSoup
from azure.storage.filedatalake import DataLakeServiceClient
from dateutil.relativedelta import relativedelta

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
file_name_config = "config_dscr_ics_qtrly_report.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON['pipeline']['raw']['snapshot_source_path']
historical_source_path = config_JSON['pipeline']['raw']['appended_path']
historical_source_file = config_JSON['pipeline']['raw']['appended_file']
sink_path = config_JSON['pipeline']['raw']['appended_path']
sink_file = config_JSON['pipeline']['raw']['appended_file']


# COMMAND ----------

# Pull new dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
file_name_list = [file for file in file_name_list if '.xlsm' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_excel(io.BytesIO(new_dataset), sheet_name = "Backsheet for Pipeline", header = 1, engine='openpyxl') 
 

# COMMAND ----------

new_source_path

# COMMAND ----------

new_dataframe

# COMMAND ----------

# Pull historical dataset
# -----------------------------------------------------------------------
latestFolder_historical = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder_historical, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")
historical_dataframe


# COMMAND ----------

#drop rows that contain no data in any of the columns
df_process = new_dataframe.copy()
df_process.dropna(axis = 0, how = 'all', inplace = True)
df_process = df_process.reset_index()
df_process


# COMMAND ----------

##process new dataframe

#drop rows that contain no data in any of the columns
df_process = new_dataframe.copy()
df_process.dropna(axis = 0, how = 'all', inplace = True)
df_process.reset_index(inplace = True)

#copy ICS overview information for each row
ics_row = df_process[['ICS Overview - Year 1 2022/23 Reporting QTR:',
 'ICS Overview  - ICS NAME:',
 'ICS Overview - LOCAL AUTHORITY NAME:',
 'ICS Overview - ICS SRO Approved prior to submission:',
 'ICS Overview - APPROVED BY (Name):',
 'ICS Overview - (Job Role):',
 'ICS Overview - Submitted to DiSC By (Name): ',
 'ICS Overview - (Job Role):.1',
 'ICS Overview  - Date of Qtrly Report Submission:',
 'ICS Overview  - DiSC Regional Lead Approved:',
 'ICS Overview - Date Reviewed :',
 'ICS Overview - QTRLY FUNDING ALLOCATION:',
 'ICS Overview - QTRLY FUNDING  APPROVED: ',
 'ICS Overview - ESCALATION REQUIRED:',
 'ICS Overview - DATE ESCALATION MTG:']].iloc[0]

for i in range(df_process.shape[0]):
  df_process.loc[i, ['ICS Overview - Year 1 2022/23 Reporting QTR:',
 'ICS Overview  - ICS NAME:',
 'ICS Overview - LOCAL AUTHORITY NAME:',
 'ICS Overview - ICS SRO Approved prior to submission:',
 'ICS Overview - APPROVED BY (Name):',
 'ICS Overview - (Job Role):',
 'ICS Overview - Submitted to DiSC By (Name): ',
 'ICS Overview - (Job Role):.1',
 'ICS Overview  - Date of Qtrly Report Submission:',
 'ICS Overview  - DiSC Regional Lead Approved:',
 'ICS Overview - Date Reviewed :',
 'ICS Overview - QTRLY FUNDING ALLOCATION:',
 'ICS Overview - QTRLY FUNDING  APPROVED: ',
 'ICS Overview - ESCALATION REQUIRED:',
 'ICS Overview - DATE ESCALATION MTG:']] = ics_row
  
df_process

# COMMAND ----------

#drop the separator columns (columns which are unnamed)
df_process = df_process[df_process.columns.drop(list(df_process.filter(regex='Unnamed')))]
df_process

# COMMAND ----------

historical_dataframe = historical_dataframe.append(df_process)
#historical_dataframe

# COMMAND ----------

historical_dataframe['Remote Monitoring Tracker -             KEY ACTIVITIES and INSIGHTS:'] = historical_dataframe['Remote Monitoring Tracker -             KEY ACTIVITIES and INSIGHTS:'].astype('string')

# COMMAND ----------

#Upload hsitorical appended data to datalake
# -----------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------


