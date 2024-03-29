# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_reference_shapefile_orchestrator.py
DESCRIPTION:
                Orchestrator databricks notebook which runs the ingestion notebooks for the download of shapefiles from the ONS Geo Portal for NHSX Analyticus Unit Dashboard projects
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli and Craig Shenton
CONTACT:        data@nhsx.nhs.uk
CREATED:        30 Aug. 2021
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
import regex as re

# 3rd party:
import pandas as pd
import numpy as np
from pathlib import Path
from azure.storage.filedatalake import DataLakeServiceClient
import requests
from urllib.request import urlopen
from urllib import request as urlreq
from bs4 import BeautifulSoup
import geojson
# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONNECTION_STRING") 

# COMMAND ----------

# MAGIC %run /Shared/databricks/au-azure-databricks-cicd/functions/dbrks_helper_functions

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/reference_tables/"
file_name_config = "config_shapefiles.json"
file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get databricksworkspace specfic path
#----------------------------------
path_start = dbutils.secrets.get(scope='DatabricksNotebookPath', key="DATABRICKS_PATH")

#Squentially run metric notebooks
#----------------------------------
for index, item in enumerate(config_JSON['pipeline']['raw']['databricks']): # get index of objects in JSON array
  try:
    notebook = config_JSON['pipeline']['raw']['databricks'][index]['databricks_notebook']
    dbutils.notebook.run(path_start+notebook, 8000) # is 120 sec long enough for timeout?
  except Exception as e:
    print(e)
    raise Exception()
