# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_nhs_app_logins_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analytics unit metrics within the topic nhs app logins count
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Everistus Oputa, Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        23 Aug. 2022
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
import requests
from pathlib import Path
from urllib import request as urlreq
from bs4 import BeautifulSoup
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
file_name_config = "config_nhs_app_logins_dbrks.json"
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

# Pull new snapshot dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
nhs_login_file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)

for new_source_file in nhs_login_file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset))

# COMMAND ----------

# Process new snapshot into required format
# -------------------------

new_dataframe['week_commencing'] = pd.to_datetime(new_dataframe['week_commencing']).dt.strftime('%Y-%m-%d')

#pivot data and clean up
new_dataframe = pd.pivot_table(new_dataframe, index = 'week_commencing', columns='metric_title')
new_dataframe.columns = new_dataframe.columns.get_level_values(1)
new_dataframe = new_dataframe.reset_index()

#drop unessecary columns
new_dataframe = new_dataframe.loc[:,['week_commencing', 'Number of authentications (of products and services) using NHS Login', 'Number of unique users (citizens) authenticating']]

#rename columns
new_dataframe = new_dataframe.rename(columns = {'Number of authentications (of products and services) using NHS Login': 'Total Logins',
                               'Number of unique users (citizens) authenticating' :'Accounts',
                               'week_commencing': '_time'})


# COMMAND ----------

# Pull historical dataset
# -----------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

# COMMAND ----------

# Combine new data with historic data
# -----------------------------------
new_dates = list(new_dataframe['_time'])
historical_dates = list(historical_dataframe['_time'])
for date in new_dates:
  if date in historical_dates:
    historical_dataframe.loc[historical_dataframe["_time"] == date, "Accounts"] = new_dataframe.loc[new_dataframe["_time"] == date, 'Accounts'].tolist()[0]
    historical_dataframe.loc[historical_dataframe["_time"] == date, "Total Logins"] = new_dataframe.loc[new_dataframe["_time"] == date, 'Total Logins'].tolist()[0]
  else:
    historical_dataframe = historical_dataframe.append(new_dataframe[new_dataframe['_time'] == date])
    historical_dataframe = historical_dataframe.reset_index(drop = True)

# COMMAND ----------

# Upload hsitorical appended data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
