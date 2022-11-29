# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dscr_results_raw.py
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
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* pandas_ods_reader

# COMMAND ----------

pip install pandas_ods_reader

# COMMAND ----------

pip install s3fs

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import requests
import time

# 3rd party:
import pandas as pd
import numpy as np
import requests
from pathlib import Path
from urllib import request as urlreq
from bs4 import BeautifulSoup
from azure.storage.filedatalake import DataLakeServiceClient
from pandas_ods_reader import read_ods
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
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())


# COMMAND ----------

file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
# new_source_path = config_JSON['pipeline']['raw']['sink_path']
new_source_path = 'land/digital_socialcare/webscrape/timestamp/csv'
# new_source_file = config_JSON['pipeline']['raw']['sink_file']
new_source_file = 'abc'
historical_source_path = config_JSON['pipeline']['raw']['appended_path']
historical_source_file = config_JSON['pipeline']['raw']['appended_file']
sink_path = config_JSON['pipeline']['raw']['appended_path']
sink_file = config_JSON['pipeline']['raw']['appended_file']

# COMMAND ----------

file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")

location_file_path_config = "/config/pipelines/nhsx-au-analytics/"
location_file_name_config = "config_digitalrecords_socialcare_dbrks.json"

location_config_JSON = datalake_download(CONNECTION_STRING, file_system_config, location_file_path_config, location_file_name_config)
location_config_JSON = json.loads(io.BytesIO(location_config_JSON).read())
location_source_path = location_config_JSON['pipeline']['project']['source_path']
location_source_file = location_config_JSON['pipeline']['project']['source_file']

location_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, location_source_path)
file = datalake_download(CONNECTION_STRING, file_system, location_source_path+location_latestFolder, location_source_file)
df_loc = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_location = df_loc[["Location ID"]]
loc_list = df_location["Location ID"].tolist()
print(df_location.head())
# print(loc_list)
print('len ', len(loc_list))




# COMMAND ----------

# Test location ids to avoid hitting API too many times
# test_loc = ['1-10000302982', '1-10000367985', '1-10000433668', '1-10000697432', '1-10000792582', '1-10000812939']
# test_loc = locations
link = 'https://api.cqc.org.uk/public/v1/locations/'
# test_loc = l
test_loc = loc_list
# Loop through location ids to retrieve full data
full_data =[]

i = 1
for locb in test_loc:
    print('loc ',i, locb , datetime.now())
    new = link + locb
    y = requests.get(new)
    print(y.status_code)
    # print(y.content)
    # print(type(y))
    y2 = y.json()
    full_data.append(y2)
    i=i+1
#     time.sleep(0.1)


# Print full data list
# print('full data = ' ,full_data)


# COMMAND ----------

# dfItem = pd.DataFrame.from_records(full_data)
x3 = json.dumps(full_data)
# print(x3)
df = pd.read_json(io.StringIO(x3), orient ='records')

# COMMAND ----------

cols = ['locationId', 'providerId', 'dormancy', 'careHome', 'inspectionCategories','inspectionDirectorate',  'onspdCcgCode','postalCode']
# 'inspectionCategories',
df2 = df[cols].copy(deep=True)
df2['inspectionCategories_name'] = df2['inspectionCategories'].apply(lambda x: x[0]['name']) 
# lists = df['inspectionCategories']
dscr_df_snapshot = df2
# display(df2)
# print(lists)

# COMMAND ----------

# Upload raw data snapshot to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
# new_source_path = config_JSON['pipeline']['raw']['snapshot_source_path']
# new_source_path = 'land/digital_socialcare/webscrape/timestamp/csv'

file_contents = io.StringIO()
dscr_df_snapshot.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, new_source_path+current_date_path, new_source_file)

# COMMAND ----------

print(file_contents, CONNECTION_STRING, file_system, new_source_path+current_date_path, new_source_file)

# COMMAND ----------

#Pull historical dataset
# latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
# historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, historical_source_file)
# historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

# # Append new data to historical data
# # -----------------------------------------------------------------------
# if dscr_df_snapshot['Date'].max() not in historical_dataframe.values:
#   historical_dataframe = historical_dataframe.append(eps_df_snapshot)
#   historical_dataframe = historical_dataframe.reset_index(drop=True)
#   historical_dataframe.index.name = "Unique ID"
# else:
#   print("data already exists")

# COMMAND ----------

# Upload processed data to datalake
# file_contents = io.BytesIO()
# historical_dataframe.to_parquet(file_contents, engine="pyarrow")
# datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
