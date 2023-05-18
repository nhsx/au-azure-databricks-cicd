# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dscr_results_raw_v3.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analyticus unit metrics within the CQC Digital Social Care Record (DSCR)
                topic
USAGE:
                ...
CONTRIBUTORS:   Everistus Oputa
CONTACT:        NHSX.Data@england.nhs.uk
CREATED:        07 Nov. 2022
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* xlrd openpyxl python-dateutil odfpy

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
file_name_config = "config_dscr_dbrks.json"
file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON['pipeline']['raw']['snapshot_source_path']
historical_source_path = config_JSON['pipeline']['raw']['appended_path']
historical_source_file = config_JSON['pipeline']['raw']['appended_file']
sink_path = config_JSON['pipeline']['raw']['appended_path']
sink_file = config_JSON['pipeline']['raw']['appended_file']

# COMMAND ----------

# Pull new snapshot dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
file_name_list = [file for file in file_name_list if '.csv' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset), encoding = "ISO-8859-1")


# COMMAND ----------

# Pull historical dataset
# -----------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

# COMMAND ----------

# Append new data to historical data
# -----------------------------------------------------------------------

# Get dates which are stored as string in the df for convertion to datetime
new_df_dates = new_dataframe[['run_date']].copy()
historical_df_dates = historical_dataframe[['run_date']].copy()

# Convert string to datetime
new_df_dates['run_date'] = pd.to_datetime(new_df_dates["run_date"])
historical_df_dates['run_date'] = pd.to_datetime(historical_df_dates['run_date'])

# Get today's run_date. run_date is added manually to the downloaded file and is same rows in a given file
today_run_date = new_df_dates['run_date'].values[0]

# If run_date is in historical file then it means the date is not new and should not be appended
if today_run_date not in historical_df_dates['run_date'].values:
  historical_dataframe = historical_dataframe.append(new_dataframe)
  historical_dataframe = historical_dataframe.sort_values(by=['run_date'])
  historical_dataframe = historical_dataframe.reset_index(drop=True)
  historical_dataframe = historical_dataframe.astype(str)
else:
  print('data already exists')

# COMMAND ----------

# Upload hsitorical appended data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
new_dataframe['Provider Charity Number'] = pd.to_numeric(new_dataframe['Provider Charity Number'], errors='coerce')
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
