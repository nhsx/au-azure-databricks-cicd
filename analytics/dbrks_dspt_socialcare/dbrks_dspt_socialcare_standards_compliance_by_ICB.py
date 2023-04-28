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
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        22 Aug 2021
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
dscr_source_path = dscr_config_JSON['pipeline']['project']['source_path']
dscr_source_file = dscr_config_JSON['pipeline']['project']['source_file']
dscr_reference_path = dscr_config_JSON['pipeline']['project']['reference_source_path']
dscr_reference_file = dscr_config_JSON['pipeline']['project']['reference_source_file']
dscr_file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
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

# COMMAND ----------

df.columns

# COMMAND ----------

df

# COMMAND ----------

# dscr data Processing
# -------------------------------------------------------------------------
dscr_latestFolder = datalake_latestFolder(CONNECTION_STRING, dscr_file_system, dscr_source_path)
ref_latestFolder = datalake_latestFolder(CONNECTION_STRING, dscr_file_system, reference_path)

dscr_file = datalake_download(CONNECTION_STRING, dscr_file_system, dscr_source_path+dscr_latestFolder, dscr_source_file)
df_dscr = pd.read_parquet(io.BytesIO(dscr_file), engine="pyarrow")

ref_file = datalake_download(CONNECTION_STRING, dscr_file_system, reference_path+ref_latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(ref_file), engine='pyarrow')

# COMMAND ----------

df_ref

# COMMAND ----------

#get CCG ONS code and CQC-location-id
df_CCG = df_dscr[['Location ID', 'Location ONSPD CCG Code']]
df_CCG = df_CCG.rename(columns={'Location ID': 'Location CQC ID ', 'Location ONSPD CCG Code':'CCG_ONS_Code'})
df_CCG = df_CCG.merge(df_ref, on='CCG_ONS_Code', how='left')
df_CCG

# COMMAND ----------

df_join = df.merge(df_CCG, on = 'Location CQC ID ', how = 'left')
df_join

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------

'''latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
'''
df = df_join
df["Count"] = 1


df_1 = df.groupby(['Date',"CQC registered location - latest DSPT status", 'ICB_Code']).sum().reset_index()


#Makes changed as outlined in the SOP when the finanical year flag for Standards Met/Exceeded changes
#----------------------------------------------------------------------------------------------------
#19/20 and 20/21 FY
#-----------------------------------
df_2 = df_1.loc[df_1['Date'] <= '2021-09']
df_3 = df_2[
(df_2["CQC registered location - latest DSPT status"] == "19/20 Standards Met.") |
(df_2["CQC registered location - latest DSPT status"] == "19/20 Standards Exceeded.") |
(df_2["CQC registered location - latest DSPT status"] == "20/21 Standards Met.") |
(df_2["CQC registered location - latest DSPT status"] == "20/21 Standards Exceeded.")
].reset_index(drop=True)
df_4 = df_3.groupby(["Date", "CQC registered location - latest DSPT status", 'ICB_Code']).sum().reset_index()

icb_list = list(df_4['ICB_Code'].unique())
for icb in icb_list:
  total = df_4.loc[df_4['ICB_Code'] == icb]['Count'].sum()
  df_4.loc[df_4['ICB_Code'] == icb, 'Total'] = total
  

#20/21 and 21/22 FY
#---------------------------------
df_5 = df_1.loc[(df_1['Date'] >= '2021-10') & (df_1['Date'] < '2022-09')] #------------- change filter once new finanical year added.
df_6 = df_5[
(df_5["CQC registered location - latest DSPT status"] == "20/21 Standards Met.") |
(df_5["CQC registered location - latest DSPT status"] == "20/21 Standards Exceeded.") |
(df_5["CQC registered location - latest DSPT status"] == "21/22 Standards Met.") |
(df_5["CQC registered location - latest DSPT status"] == "21/22 Standards Exceeded.")
].reset_index(drop=True)
df_7 = df_6.groupby(["Date", "CQC registered location - latest DSPT status", 'ICB_Code']).sum().reset_index()

icb_list = list(df_7['ICB_Code'].unique())
for icb in icb_list:
  total = df_7.loc[df_7['ICB_Code'] == icb]['Count'].sum()
  df_7.loc[df_7['ICB_Code'] == icb, 'Total'] = total

#21/22 and 22/23 FY
#---------------------------------
df_8 = df_1.loc[df_1['Date'] >= '2022-09']
df_9 = df_8[
(df_8["CQC registered location - latest DSPT status"] == "21/22 Standards Met.") |
(df_8["CQC registered location - latest DSPT status"] == "21/22 Standards Exceeded.") |
(df_8["CQC registered location - latest DSPT status"] == "22/23 Standards Met.") |
(df_8["CQC registered location - latest DSPT status"] == "22/23 Standards Exceeded.")
].reset_index(drop=True)
df_10 = df_9.groupby(["Date", "CQC registered location - latest DSPT status", 'ICB_Code']).sum().reset_index()

icb_list = list(df_10['ICB_Code'].unique())
for icb in icb_list:
  total = df_10.loc[df_10['ICB_Code'] == icb]['Count'].sum()
  df_10.loc[df_10['ICB_Code'] == icb, 'Total'] = total

#---------------------------------------------------------------------------------------
df_fy_appended = pd.concat([df_4, df_7, df_10]).reset_index(drop = True) #------------- Once a new finanical year is added appened additional FY dataframe. ie pd.concat([df_4, df_7, df_10]).reset_index(drop = True)
#---------------------------------------------------------------------------------------
#df_dates = df_1.groupby("Date").sum().reset_index()
#df_dates_merged = df_fy_appended.merge(df_dates, on = 'Date', how = 'left')
#df_dates_merged_1 = df_dates_merged.rename(columns = { 'Count_x':'Number of social care organizations with a standards met or exceeded DSPT status', 'Count_y':'Total number of social care organizations'})
#df_dates_merged_1["Percent of social care organizations with a standards met or exceeded DSPT status"] = df_dates_merged_1['Number of social care organizations with a standards met or exceeded DSPT status']/df_dates_merged_1['Total number of social care organizations']
#df_dates_merged_2 = df_dates_merged_1.round(4)
#df_dates_merged_2["Date"] = pd.to_datetime(df_dates_merged_2["Date"])
#df_dates_merged_2.index.name = "Unique ID"
#df_processed = df_dates_merged_2.copy()

df_fy_appended['Date'] = pd.to_datetime(df_fy_appended['Date'])
df_processed = df_fy_appended

# COMMAND ----------

#21/22 and 22/23 FY
#---------------------------------
df_latest = df_1.loc[df_1['Date'] >= '2022-09']
df_latest = df_latest .groupby(["Date", "CQC registered location - latest DSPT status", 'ICB_Code']).sum().reset_index()

icb_list = list(df_latest['ICB_Code'].unique())
for icb in icb_list:
  total = df_latest.loc[df_latest['ICB_Code'] == icb]['Count'].sum()
  df_latest.loc[df_latest['ICB_Code'] == icb, 'Total'] = total

df_latest['Total'] = df_latest['Total'].astype(int)
df_latest['CQC registered location - latest DSPT status'] = df_latest['CQC registered location - latest DSPT status'].str.replace('.', '')

df_latest['Date'] = pd.to_datetime(df_latest['Date'])
df_latest = df_latest.rename(columns = {'Date':'Report Date', 'CQC registered location - latest DSPT status':'Standard status', 'Count':'Number of locations with the standard status', 'Total':'Total number of locations'})

df_latest


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

# COMMAND ----------


