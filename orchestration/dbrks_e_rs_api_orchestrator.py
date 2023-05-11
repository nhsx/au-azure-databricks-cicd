# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_e_rs_api_orchestrator.py
DESCRIPTION:
                Orchestrator databricks notebook which runs the processing notebooks for DCT metrics within the E_rs_Api
USAGE:
                ...
CONTRIBUTORS:   Everistus Oputa 
CONTACT:        data@nhsx.nhs.uk
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

#Download JSON config from Azure datalake
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_ers_api.json"
file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get databricksworkspace specfic path
#---------------------------------
path_start = dbutils.secrets.get(scope='DatabricksNotebookPath', key="DATABRICKS_PATH")

#Squentially run metric notebooks
#---------------------------------
for index, item in enumerate(config_JSON['pipeline']['project']['databricks']): # get index of objects in JSON array
    try:
        notebook = config_JSON['pipeline']['project']['databricks'][index]['databricks_notebook']
        dbutils.notebook.run(path_start+notebook, 8000) # is 120 sec long enough for timeout?
    except Exception as e:
        print(e)
        raise Exception()
