# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_secondarycare_virtual_ward_patients_biweek_rate.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX dfpc analytics metric: Number of patients on virtual ward per 1,000 registered population, biweekly 
USAGE:
                
CONTRIBUTORS:   Everistus Oputa
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        23 Feb 2023
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

# COMMAND ----------

# MAGIC %run /Shared/databricks/au-azure-databricks-cicd/functions/dbrks_helper_functions

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_virtual_ward_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_path = config_JSON['pipeline']['project']['reference_source_path']
reference_file = config_JSON['pipeline']['project']['reference_source_file']
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
sink_path = config_JSON['pipeline']['project']['databricks'][1]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][1]['sink_file']
table_name = config_JSON['pipeline']['staging'][1]['sink_table']

# COMMAND ----------

#Processing
# -------------------------------------------------------------------------------------------------
df1 = df[["Period_End", "ICB_Code", "current_capacity","total_number_of_patients_on_ward"]].copy()
df1['total_number_of_patients_on_ward'] = df1['total_number_of_patients_on_ward'].astype(int)
df1 = df1.groupby(['Period_End','ICB_Code','current_capacity'], as_index=False).sum()
df1['Period_End'] = pd.to_datetime(df1['Period_End'], infer_datetime_format=True)
df2 = df1[df1['Period_End'] >= '2021-01-01'].reset_index(drop = True)  #--------- taking dates from 
df3 = df2[['Period_End','ICB_Code', 'current_capacity', 'total_number_of_patients_on_ward']]
df4 = df3.rename(columns = {'Period_End': 'Biweekly Date','ICB_Code': 'ORG_CODE', 'current_capacity': 'Virtual Ward Current Capacity','total_number_of_patients_on_ward': 'Total Number of Patients on Virtual Ward'})


# ref data Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system,reference_path+latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref_1 = df_ref[['EXTRACT_DATE','ORG_TYPE','ORG_CODE','SEX','AGE','NUMBER_OF_PATIENTS']]
df_ref_1['NUMBER_OF_PATIENTS'] = df_ref_1['NUMBER_OF_PATIENTS'].astype(int)
df_ref_1 = df_ref_1[(df_ref_1['ORG_TYPE']=='STP') & (df_ref_1['SEX']=='FEMALE','MALE')]
df_ref_1 = df_ref_1.groupby(['EXTRACT_DATE','ORG_TYPE','ORG_CODE','SEX','AGE'], as_index=False).sum()
df_ref_1['EXTRACT_DATE'] = pd.to_datetime(df1['EXTRACT_DATE'], infer_datetime_format=True)
df1 = df_ref_1[(df_ref_1['AGE'] >'16') & (df_ref_1['AGE']!== 'ALL')].reset_index(drop = True)  #---------required age for virtual ward stay above 16 and ALLin the file to be eliminated 
df2 = df1[['EXTRACT_DATE','ORG_TYPE','ORG_CODE','SEX','AGE','NUMBER_OF_PATIENTS']]




# COMMAND ----------

# Joint processing
# -------------------------------------------------------------------------
df_join = df4.merge(df2, how ='outer', on = 'ORG_CODE')
df_join.index.name = "Unique ID"
df_join = df_join.round(4)


# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
