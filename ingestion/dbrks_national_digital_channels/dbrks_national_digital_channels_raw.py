# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_national_digital_channels_raw.py
DESCRIPTION:
                Databricks notebook with code to ingest new raw data for the NHSX Analytics unit metrics within 
                the National Digital Channels (NDC) Dashboard Porject
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli, Kabir Khan
CONTACT:        data@nhsx.nhs.uk
CREATED:        25th Aug 2022
VERSION:        0.0.2
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* xlrd openpyxl python-dateutil

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
file_name_config = "config_national_digital_channels_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON['pipeline']['raw']['source_path']
appended_path = config_JSON['pipeline']['raw']['appended_path']
appended_daily_file = config_JSON['pipeline']['raw']['appended_file_daily']
appended_monthly_file = config_JSON['pipeline']['raw']['appended_file_monthly']
appended_ods_file = config_JSON['pipeline']['raw']['appended_file_daily_ods']
appended_messages_file = config_JSON['pipeline']['raw']['appended_file_messages']
appended_forecasts_file = config_JSON['pipeline']['raw']['appended_file_forecasts']

# COMMAND ----------

#Pull Excel file
# ----------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
source_file  = [file for file in file_name_list if '.xlsx' in file][0]
new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, source_file)

# COMMAND ----------

# Pull daily dataset
# ----------------------------------------
new_data = pd.read_excel(io.BytesIO(new_dataset), sheet_name = ['NHS App data file', 'vaccinations', 'EPS'], engine='openpyxl')
new_data_df = pd.DataFrame()
for sheet_name, df in new_data.items():
  if new_data_df.empty:
    new_data_df = new_data_df.append(df)
  else:
    new_data_df = new_data_df.merge(df, how='outer', on = 'Daily')
daily_raw_df = new_data_df.copy()  

# Upload merged data to datalake
# -------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
daily_raw_df.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, appended_path+current_date_path, appended_daily_file)

# COMMAND ----------

# Pull monthly dataset
# ----------------------------------------
new_data_month = pd.read_excel(io.BytesIO(new_dataset), sheet_name = ['jumpoffs', 'NHS App Dash', 'NHS UK', 'NHS UK Site Sections', 'Appts in Primary Care', 'NHS Login report', 'NHS.UK report'], engine='openpyxl')
new_data_df_month = pd.DataFrame()
for sheet_name, df in new_data_month.items():
  if new_data_df_month.empty:
    new_data_df_month = new_data_df_month.append(df)
  else:
    new_data_df_month = new_data_df_month.merge(df, how='outer', on = 'Monthly')
monthly_raw_df = new_data_df_month.copy()  

# Upload merged data to datalake
# -------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
monthly_raw_df.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, appended_path+current_date_path, appended_monthly_file)

# COMMAND ----------

# Pull forecasts dataset
# ----------------------------------------
new_data_forecasts = pd.read_excel(io.BytesIO(new_dataset), sheet_name = ['Forecasts'], engine='openpyxl')
new_data_df_forecasts = pd.DataFrame()
for sheet_name, df in new_data_forecasts.items():
  if new_data_df_forecasts.empty:
    new_data_df_forecasts = new_data_df_forecasts.append(df)
  else:
    new_data_df_forecasts = new_data_df_forecasts.merge(df, how='outer', on = 'Monthly')
forecasts_raw_df = new_data_df_forecasts.copy()  

# Upload merged data to datalake
# -------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
forecasts_raw_df.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, appended_path+current_date_path, appended_forecasts_file)

# COMMAND ----------

# Pull ODS dataset
# ----------------------------------------
new_data_ods = pd.read_excel(io.BytesIO(new_dataset), sheet_name = 'econsult', engine='openpyxl')
new_data_ods = new_data_ods.loc[:, ~new_data_ods.columns.str.contains('^Unnamed')]
new_data_ods['day'] = pd.to_datetime(new_data_ods['day'])
new_data_ods_df = new_data_ods.copy()

# Upload merged data to datalake
# -------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
new_data_ods_df.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, appended_path+current_date_path, appended_ods_file)

# COMMAND ----------

#Pull messages dataset
# ----------------------------------------
new_data_messages = pd.read_excel(io.BytesIO(new_dataset), sheet_name = 'Messaging & Notifications', engine='openpyxl')
new_data_messages['Date'] = pd.to_datetime(new_data_messages['Date'])
new_data_messages_df = new_data_messages.copy()

# Upload merged data to datalake
# -------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
new_data_messages_df.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, appended_path+current_date_path, appended_messages_file)
