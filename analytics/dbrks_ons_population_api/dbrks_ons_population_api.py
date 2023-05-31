# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_ons_population_api.py
DESCRIPTION:
                Databricks notebook with code to process raw ons population figures
USAGE:         
CONTRIBUTORS:   Oli Jones
CONTACT:        data@nhsx.nhs.uk
CREATED:        30 May 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* geopandas shapely geopandas shapely 

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
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape
from shapely import wkb, wkt
import shapely.speedups
shapely.speedups.enable()
# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Shared/databricks/au-azure-databricks-cicd/functions/dbrks_helper_functions

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/reference_tables/"
file_name_config = "config_ons_population_api.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
shapefile_source_path = config_JSON['pipeline']['project'][0]['source_path']
shapefile_source_file = config_JSON['pipeline']['project'][0]['source_file']
shapefile_sink_path = config_JSON['pipeline']['project'][0]['sink_path']
shapefile_sink_file = config_JSON['pipeline']['project'][0]['sink_file']

# COMMAND ----------

#Processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, shapefile_source_path)
file = datalake_download(CONNECTION_STRING, file_system, shapefile_source_path+latestFolder, shapefile_source_file)
# df = gpd.read_file(io.BytesIO(file))
df = pd.read_parquet(file)
# shapefile_source_path 
# shapefile_source_file
# shapefile_sink_path 
# shapefile_sink_file


# df_processed = df_2.copy()

# COMMAND ----------

latestFolder

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, shapefile_sink_path+latestFolder, shapefile_sink_file)
