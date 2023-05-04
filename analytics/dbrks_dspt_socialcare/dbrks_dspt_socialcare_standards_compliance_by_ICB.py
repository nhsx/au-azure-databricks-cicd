# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_cybersecurity_dspt_care_standards_year_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: Number and percent of adult social care organisations that meet or exceed the DSPT standard (M011 & M012)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Faaiz Muhammad , Everistus Oputa
CONTACT:        data@nhsx.nhs.uk
CREATED:        04 May 2023
VERSION:        0.0.3
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
file_name_config = "config_dspt_socialcare_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

#Load DSCR config from Azure datalake
# -------------------------------------------------------------------------
dscr_file_path_config = "/config/pipelines/nhsx-au-analytics/"
dscr_file_name_config = "config_dscr_dbrks.json"
dscr_file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
dscr_config_JSON = datalake_download(CONNECTION_STRING, dscr_file_system_config, dscr_file_path_config, dscr_file_name_config)
dscr_config_JSON = json.loads(io.BytesIO(dscr_config_JSON).read())

# COMMAND ----------

# Read parameters from JSON configs
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
sink_path = config_JSON['pipeline']['project']['databricks'][1]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][1]['sink_file']
table_name = config_JSON['pipeline']["staging"][1]['sink_table']

#Get parameters from JSON config
# -------------------------------------------------------------------------
dscr_source_path = dscr_config_JSON['pipeline']['raw']['snapshot_source_path']
dscr_source_file = dscr_config_JSON['pipeline']['raw']['appended_file']
dscr_reference_path = dscr_config_JSON['pipeline']['project']['reference_source_path']
dscr_reference_file = dscr_config_JSON['pipeline']['project']['reference_source_file']
dscr_file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
#dscr_sink_path = dscr_config_JSON['pipeline']['project']['databricks'][1]['sink_path']
#dscr_sink_file = dscr_config_JSON['pipeline']['project']['databricks'][1]['sink_file']
dscr_sink_path = dscr_config_JSON['pipeline']['project']['databricks'][1]['sink_path']
dscr_sink_file = dscr_config_JSON['pipeline']['project']['databricks'][1]['sink_file']
reference_path = dscr_config_JSON['pipeline']['project']['reference_source_path']
reference_file = dscr_config_JSON['pipeline']['project']['reference_source_file']

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")


latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, dscr_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, dscr_source_path+latestFolder)
file_name_list = [file for file in file_name_list if '.csv' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, dscr_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset), encoding = "ISO-8859-1")

# COMMAND ----------

# dscr data Processing
# -------------------------------------------------------------------------
dscr_latestFolder = datalake_latestFolder(CONNECTION_STRING, dscr_file_system, dscr_source_path)
ref_latestFolder = datalake_latestFolder(CONNECTION_STRING, dscr_file_system, reference_path)

dscr_file = datalake_latestFolder(CONNECTION_STRING, file_system, dscr_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, dscr_source_path+latestFolder)
file_name_list = [file for file in file_name_list if '.csv' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, dscr_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset), encoding = "ISO-8859-1")

ref_file = datalake_download(CONNECTION_STRING, dscr_file_system, reference_path+ref_latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(ref_file), engine='pyarrow')

# COMMAND ----------

#get CCG ONS code and CQC-location-id
df_CCG = new_dataframe[['Location ID', 'Location ONSPD CCG Code']]
df_CCG = df_CCG.rename(columns={'Location ID': 'Location CQC ID ', 'Location ONSPD CCG Code':'CCG_ONS_Code'})
df_CCG = df_CCG.merge(df_ref, on='CCG_ONS_Code', how='left')
#df_CCG

# COMMAND ----------

df_join = df.merge(df_CCG, on = 'Location CQC ID ', how = 'left')
#df_join

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------

'''latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
'''
df = df_join
df["Count"] = 1


#df_1 = df.groupby(['Date',"CQC registered location - latest DSPT status", 'ICB_Code']).sum().reset_index()



# COMMAND ----------


#uncomment this if on 21/22/ and 22/23 status are needed 
#---------------------------------------------------------------------------------------------------------------------------------------
# df = df[df["CQC registered location - latest DSPT status"].isin(["21/22 Approaching Standards.", 
#                                                                    "21/22 Standards Exceeded.", 
#                                                                    "21/22 Standards Met.", 
#                                                                    "21/22 Standards Not Met.",
#                                                                    "22/23 Approaching Standards.",
#                                                                    "22/23 Standards Exceeded.",
#                                                                    "22/23 Standards Me.",
#                                                                    "Not Individually Registered.",                                                     
#                                                                    "Not Published." ])].reset_index(drop=True) #------ select required FY for the DSPT standard 

                                                                  

# Generating Number of Social Care Organisation Dspt Status
df_latest = df[["Date","CQC registered location - latest DSPT status","ICB_Code"]].copy()
df_latest = df.loc[df['Date'] >= '2022-09']
df_latest['CQC registered location - latest DSPT status'] = df_latest['CQC registered location - latest DSPT status'].astype(str)
df_latest = df_latest.groupby(['Date','ICB_Code','CQC registered location - latest DSPT status'], as_index=False).size()
df5 = df_latest[['Date','ICB_Code','CQC registered location - latest DSPT status','size']]
#df6 = df5.rename(columns = {'Date': 'Date','ICB_Code':'ICB_Code','CQC registered location - latest DSPT status':'ICB_Code':'CQC registered location - latest DSPT status','size':'Total'})


# Generating Total number of socialcare organisation per ICB by date
df_latest = df[["Date", "ICB_Code"]].copy()
df_latest = df.loc[df['Date'] >= '2022-09']
df_latest['ICB_Code'] = df_latest['ICB_Code'].astype(str)
df_latest = df_latest.groupby(['Date','ICB_Code'], as_index=False).size()
df2 = df_latest[['Date','ICB_Code','size']]
df3 = df2.rename(columns = {'Date': 'Date','ICB_Code':'ICB_Code','size':'Total'})


#Joined data processing for social care dspt status and Total number social care organisation
df_join = pd.merge(df3,df5, on = ['ICB_Code','Date']) 
df_join_1 = df_join.rename(columns = {'Date':'Report Date','ICB_Code': 'ICB_CODE','CQC registered location - latest DSPT status': 'Standard status','size':'Number of locations with the standard status','Total':'Total number of locations'})
# df_join_1["Percent of Trusts with a standards met or exceeded DSPT status"] = df_join_1["Number of Trusts with the standard status"]/df_join_1["Total number of Trusts"]  (apply similar formular for social care if the percentage is needed as well)
df_join_1 = df_join_1.round(2)
#df_join_1['Report Date'] = pd.to_datetime(df_join_1['Report Date'])
df_join_1.index.name = "Unique ID"
df_processed = df_join_1.copy()



# COMMAND ----------

display(df_processed )

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
df_latest.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_latest, table_name, "overwrite")
