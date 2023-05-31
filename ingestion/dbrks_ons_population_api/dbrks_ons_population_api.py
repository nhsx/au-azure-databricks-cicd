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
                Databricks notebook with code to pull ICB population from the ONS API
USAGE:
                ...
CONTRIBUTORS:   Oliver Jones
CONTACT:        data@nhsx.nhs.uk
CREATED:        30 May 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# ------------------------------------------------------------------------------------
%pip install pandas pathlib azure-storage-file-datalake numpy pyarrow==5.0.*

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
import json
import requests as rq

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
new_source_path = config_JSON['pipeline']['raw']['databricks'][0]['code_maping_sink_path']
sink_path = config_JSON['pipeline']['raw']['databricks'][0]['code_maping_sink_path']
sink_file = config_JSON['pipeline']['raw']['databricks'][0]['code_maping_sink_file']

# COMMAND ----------

# Pull new snapshot dataset from API
# -------------------------

link = 'https://www.nomisweb.co.uk/api/v01/dataset/NM_2300_1.data.csv?date=latest&geography=696254465...696254506&c2021_age_92=0,19...91&c_sex=0...2&measures=20100'

api_data = rq.get(link)

data = api_data.content
rawData = pd.read_csv(io.StringIO(data.decode('utf-8')))


# COMMAND ----------

# Upload processed data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
rawData.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
