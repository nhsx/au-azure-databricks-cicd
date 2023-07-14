# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dscr_all_variables_care_home_beds_consolidated.py
DESCRIPTION:
                Databricks notebook that consolidate all the final out (files in proc/project) of dbrks_dscr_all_variables_care_home_beds from previous months into one file 
                ...
CONTRIBUTORS:   Abdu Nuhu
CONTACT:        data@nhsx.nhs.uk
CREATED:        14 July. 2023
VERSION:        0.0.1
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
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")   


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

# Getting the source data file name and path, source is the output dbrks_dscr_all_variables_care_home_beds stored in proc/project
proc_project_path = config_JSON['pipeline']["project"]["databricks"][1]['sink_path']
proc_project_file = config_JSON['pipeline']["project"]["databricks"][1]['sink_file']

# Get name of output for this notebook from config
# sink_file = config_JSON['pipeline']['project']['databricks'][4]['sink_file']

#table_name = config_JSON['pipeline']["staging"][4]['sink_table']

print(proc_project_path)
print(proc_project_file )

# COMMAND ----------

service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
file_system_client = service_client.get_file_system_client(file_system=file_system)
pathlist = list(file_system_client.get_paths(proc_project_path))
folders = []

# remove file_path and source_file from list
for path in pathlist:
    folders.append(path.name.replace(proc_project_path.strip("/"), "").lstrip("/").rsplit("/", 1)[0])
    folders.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"), reverse=True)
 
print(folders)

# COMMAND ----------

data = []

for csv_folder in folders:
  full_path = proc_project_path + csv_folder
  csv_file = datalake_download(CONNECTION_STRING, file_system, full_path, proc_project_file)
  df = pd.read_csv(io.BytesIO(csv_file),  index_col=None, header=0)
  data.append(df)

frame = pd.concat(data, axis=0, ignore_index=True)
df_frame = frame.iloc[:,1:]

list(df_frame.columns.values)

# COMMAND ----------

print(df_frame["monthly_date"].unique())

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
df_frame.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Write metrics to database
# -------------------------------------------------------------------------
write_to_sql(df_frame, table_name, "overwrite")
