# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_pomi_patient_record_func_month_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: No. and % of patients registered for detailed coded record functionality (M0142)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Kabir Khan
CONTACT:        data@nhsx.nhs.uk
CREATED:        23 Aug 2022
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
file_name_config = "config_pomi_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
sink_path = config_JSON['pipeline']['project']['databricks'][14]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][14]['sink_file'] 
table_name = config_JSON['pipeline']['staging'][14]['sink_table']

# COMMAND ----------

#Processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df1 = df[['Report_Period_End', 'Practice_Code', 'Field', 'Value']]
df_num = df1[df1["Field"] == "Pat_DetCodeRec_Enbld"]
df_num_1 = df_num.rename(columns = {'Value': 'Number of patients registered for detailed coded record functionality'}).drop(columns = ['Field']).reset_index(drop = True)
df_denom = df1[df1["Field"] == "patient_list_size"]
df_denom_1 = df_denom.rename(columns = {'Value': 'Number of registered patients'}).drop(columns = ['Field']).reset_index(drop = True)
df_join = pd.merge(df_num_1, df_denom_1,  how='left', left_on=['Report_Period_End','Practice_Code'], right_on = ['Report_Period_End','Practice_Code'])
df_join["Percent of patients registered for detailed coded record functionality"] = df_join["Number of patients registered for detailed coded record functionality"]/df_join["Number of registered patients"]
df_join.rename(columns={"Report_Period_End": "Date", "Practice_Code": "Practice code"}, inplace=True)
df_join_1 = df_join[~(df_join['Percent of patients registered for detailed coded record functionality'] > 1)].reset_index(drop = True)
df_join_2 = df_join_1.round(4)
df_join_2.index.name = "Unique ID"
df_processed = df_join_2.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
