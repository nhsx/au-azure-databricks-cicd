# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_virtual_ward_biweekly_capacity_and_prop.py
              
DESCRIPTION:
                Databricks notebook with processing code for the NHSX dfpc analytics metric: virtual wards biweek capacity and the capacity (per 100,000) count
USAGE:
                ...
CONTRIBUTORS:   Everistus Oputa
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        31 May 2023
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

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_virtual_ward_capacity.json"
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
sink_path = config_JSON['pipeline']['project']['databricks'][0]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][0]['sink_file']
table_name = config_JSON['pipeline']['staging'][0]['sink_table'] 

# COMMAND ----------


 #Numerator data ingestion and processing
# -------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_csv(io.BytesIO(file))
df1 = df[["ICS_Code", "Date","Capacity"]].copy()
df1 = df1[df1['Capacity'].notna()]
df1['Capacity'] = df1['Capacity'].astype(int)
df1 = df1.groupby(['ICS_Code','Date'], as_index=False).sum()
df1['Date'] = pd.to_datetime(df1['Date'], infer_datetime_format=True)
df2 = df1[df1['Date'] >= '2022-05-20'].reset_index(drop = True)  #--------- taking dates from 
df3 = df2[['Date','ICS_Code', 'Capacity']]
df4 = df3.rename(columns = {'Date': 'Biweekly Date','ICS_Code': 'ORG_CODE','Capacity': 'Virtual Ward Capacity'})
#df4.index.name = "Unique ID"
#df_processed = df4.copy()

#Denominator data ingestion and processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system,reference_path+latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref_1 = df_ref[['EXTRACT_DATE','ORG_TYPE','ORG_CODE','SEX','AGE','NUMBER_OF_PATIENTS']]
df_ref_1['NUMBER_OF_PATIENTS'] = df_ref_1['NUMBER_OF_PATIENTS'].astype(int)
df_ref_1 = df_ref_1[(df_ref_1['ORG_TYPE']=='STP') & (df_ref_1['SEX'].isin(['FEMALE','MALE']))]
df_ref_1.replace('95+', 95, inplace=True)
df_ref_1.drop(df_ref_1[df_ref_1['AGE'] == 'ALL'].index, inplace = True)
df_ref_1["AGE"] = pd.to_numeric(df_ref_1["AGE"])
df_ref_1 = df_ref_1.loc[df_ref_1["AGE"] >= 16] #---------required age for virtual ward stay is 16+ and ALLin the file to be eliminated 
df_ref_1 = df_ref_1.drop('AGE', axis=1)
df_ref_1 = df_ref_1.groupby(['EXTRACT_DATE','ORG_CODE'], as_index=False).sum()
df_ref_1['EXTRACT_DATE'] = pd.to_datetime(df_ref_1['EXTRACT_DATE'], infer_datetime_format=True)
df2 = df_ref_1[['EXTRACT_DATE','ORG_CODE','NUMBER_OF_PATIENTS']]

#Joined data processing
df_join = df2.merge(df4, how ='outer', on = 'ORG_CODE')
df_join_1 = df_join.drop(columns = ['EXTRACT_DATE']).rename(columns = {'Biweekly Date':'Biweekly Date','ORG_CODE': 'ICB_CODE','NUMBER_OF_PATIENTS': 'ICB Population)','Virtual Ward Capacity':'Virtual Ward Capacity at ICB level'})
df_join_1['VW Capacity (per 100,000 )'] = df_join_1["Virtual Ward Capacity at ICB level"]/(df_join_1['ICB Population)']/100000)
df_join_2 = df_join_1.round(2)
df_join_2.index.name = "Unique ID"
df_join_2["Date"] = pd.to_datetime(df_join_2["Date"])
df_processed = df_join_2.copy()



# COMMAND ----------

df_processed

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
