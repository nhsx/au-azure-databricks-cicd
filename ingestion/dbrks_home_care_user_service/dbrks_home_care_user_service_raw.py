# Databricks notebook source
#!/usr/bin python3

# --------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
FILE:           dbrks_home_care_user_service_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical for home care service users
              
USAGE:
                ...
CONTRIBUTORS:   Kabir Khan
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        22 Feb 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas==1.5 pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* xlrd openpyxl python-dateutil fastparquet

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
file_name_config = "config_home_care_user_service.json"
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

# Pull new dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+ latestFolder)
dom_list = [file for file in file_name_list if 'Home Care' in file]
res_list = [file for file in file_name_list if 'Care Home' in file]

for dom_source_file in dom_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, dom_source_file)
  df_dom = pd.read_excel(io.BytesIO(new_dataset), engine='openpyxl')

for res_source_file in res_list:
  care_home_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, res_source_file)
  df_res = pd.read_excel(io.BytesIO(new_dataset), engine='openpyxl')

# COMMAND ----------

#get the max date in the 'CqcSurveyLastUpdatedBst column and make this the new date column 
df_dom['Date'] = pd.to_datetime(df_dom['CqcSurveyLastUpdatedBst'], dayfirst=True).dt.date.max().strftime('%d-%m-%Y')

# COMMAND ----------

#set IsActive and IsDomcare to 1 for df_dom
df_dom['IsActive'] = 1
df_dom['IsDomcare'] = 1

# COMMAND ----------

#only retain the relavant columns
df_dom = df_dom[['Date', 'CqcId', 'IsActive', 'IsDomcare']]

# COMMAND ----------

#get the max date in the 'CqcSurveyLastUpdatedBst column and make this the new date column for df_res
df_res['Date'] = pd.to_datetime(df_res['CqcSurveyLastUpdatedBst'], dayfirst=True).dt.date.max().strftime('%d-%m-%Y')

# COMMAND ----------

#set IsActive 1 and IsDomcare to 0 for df_res
df_res['IsActive'] = 1
df_res['IsDomcare'] = 0

# COMMAND ----------

#keep relevant columns in df_res
df_res = df_res[['Date', 'CqcId', 'IsActive', 'IsDomcare']]

# COMMAND ----------

#stack df_dom and df_res together
df_hcsu = pd.concat([df_dom, df_res])

# COMMAND ----------

# Pull historical dataset
# -----------------------------------------------------------------------
latestFolder_historical = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder_historical, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")


# COMMAND ----------

# Append new data to the historical dataframe
# -----------------------------------------------------------------------
dates_in_historical = historical_dataframe["Date"].unique().tolist()
dates_in_new = df_hcsu["Date"].unique().tolist()  #[-1]
if dates_in_new == dates_in_historical:
  print('Data already exists in historical data')
else:
  historical_dataframe = historical_dataframe.append(df_hcsu)
  historical_dataframe = historical_dataframe.sort_values(by=['Date'])
  historical_dataframe = historical_dataframe.reset_index(drop=True)


# COMMAND ----------

#Upload hsitorical appended data to datalake
# -----------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
