# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) |2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_e_rs_api_month_prop.py
DESCRIPTION:
                Databricks notebook with processing code for DCT metric: ERS Api Month Prop (M387B)
USAGE:
                ...
CONTRIBUTORS:   Everistus Oputa, Muhammad-Faaiz Shanawas
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        11 May 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 openpyxl numpy urllib3 lxml regex pyarrow==8.0.*

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

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_esr_api.json"
file_system_config = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_path = config_JSON['pipeline']['project']['denominator_source_path']
reference_file = config_JSON['pipeline']['project']['denominator_source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][3]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][3]['sink_file']
table_name = config_JSON['pipeline']['staging'][3]['sink_table'] 

# COMMAND ----------


#  #Numerator data ingestion and processing
# # -------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_csv(io.BytesIO(file),encoding='ISO-8859-1')

# COMMAND ----------

df = df.rename(columns = {'ODS\xa0':'ODS'})

#remove all the spaces from the end of the ODS codes
list_of_ODS = list(df['ODS'])
for i in list_of_ODS:
  if '\xa0' in i:
    list_of_ODS[list_of_ODS.index(i)] = i.replace('\xa0', '')

#replace the ODS codes without spaces in the dataframe
df['ODS'] = list_of_ODS
df

# COMMAND ----------

#Denominator data ingestion and processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system,reference_path+latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref = df_ref.rename(columns = {'Organisation_Code':'ODS'})

# COMMAND ----------

#filter denominator for acute trusts and 'Effective_To' == None
df_acute = df_ref.loc[df_ref['NHSE_Organisation_Type'] == 'ACUTE TRUST']
df_acute = df_acute[df_acute['Effective_To'].isnull()]
df_acute  = df_acute[['ODS', 'STP_Code']]


# COMMAND ----------

df_merged = df_ref.merge(df, on = 'ODS', how = 'right')
#df_merged.loc[df_merged['STP_Code'].isna()]
df_merged

# COMMAND ----------

df_output = df.groupby(['Report_End _Date']).count()
df_output = df_output[['ODS\xa0']]
df_output = df_output.rename(columns = {'ODS\xa0':'Organisation Count'})
df_output['Acute Trusts Count'] = df_acute['NHSE_Organisation_Type'][0]
df_output = df_output.reset_index()
df_output = df_output.rename(columns = {'Report_End _Date':'Date'})


# COMMAND ----------

df

# COMMAND ----------

# Upload output dataframe to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_output.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_output, table_name, "overwrite")

# COMMAND ----------


