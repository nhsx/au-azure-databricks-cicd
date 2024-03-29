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
import calendar
from pandas.tseries.offsets import MonthEnd

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

# dscr data Processing
# -------------------------------------------------------------------------
dscr_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, dscr_source_path)
ref_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)

dscr_file = datalake_latestFolder(CONNECTION_STRING, file_system, dscr_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, dscr_source_path+dscr_latestFolder)

file_name_list = [file for file in file_name_list if '.csv' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, dscr_source_path+dscr_latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset), encoding = "ISO-8859-1")

ref_file = datalake_download(CONNECTION_STRING, file_system, reference_path+ref_latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(ref_file), engine='pyarrow')


# COMMAND ----------

#get CCG ONS code and CQC-location-id
df_CCG = new_dataframe[['Location ID', 'Location ONSPD CCG Code']]
df_CCG = df_CCG.rename(columns={'Location ID': 'Location CQC ID ', 'Location ONSPD CCG Code':'CCG_ONS_Code'})
df_CCG = df_CCG.merge(df_ref, how='left', left_on='CCG_ONS_Code', right_on='CCG21CD')
df_CCG = df_CCG.drop('CCG_ONS_Code_x', axis=1)
df_CCG = df_CCG.rename(columns={'CCG_ONS_Code_y': 'CCG_ONS_Code'})


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

df['Date']

# COMMAND ----------



df = df[["Date","CQC registered location - latest DSPT status","ICB_Code"]]
#df = df['CQC registered location - latest DSPT status'].astype(str)

#generate df for 21/22 standards only
df1 = df[df["CQC registered location - latest DSPT status"].isin(["21/22 Approaching Standards.", 
                                                                   "21/22 Standards Exceeded.", 
                                                                   "21/22 Standards Met.", 
                                                                   "21/22 Standards Not Met.",
                                                                   "22/23 Approaching Standards.",
                                                                   "22/23 Standards Exceeded.",
                                                                   "22/23 Standards Met.",
                                                                   "Not Individually Registered.",                                                     
                                                                   "Not Published." ])].reset_index(drop=True)     
                                   

df1 = df1.loc[df1['Date'] >= '2022-09']    
df2 = df1.groupby(['Date','ICB_Code'], as_index=False).size()      
df1 = df1.groupby(['Date', 'ICB_Code','CQC registered location - latest DSPT status'], as_index=False).size()                                              
df1 = df1.rename(columns = {'size':'Number of locations with standard status'})
df1 = df1.merge(df2, on = ['ICB_Code', 'Date'], how = 'left')
df1 = df1.rename(columns = {'size':'Total number of locations'})

df_processed = df1.copy()
df_processed['Date'] = df_processed['Date'] + '-01'



# COMMAND ----------

today = str(datetime.today().strftime('%Y-%m-%d'))
#today = '2023-06-29'

today_year = today[0:4]
today_month = int(today[5:7])
report_month = today_month - 1
report_date_str = today_year + '-' + str(report_month) + '-01'

report_date = datetime.strptime(report_date_str, '%Y-%m-%d')
report_last_day = report_date.replace(day = calendar.monthrange(report_date.year, report_date.month)[1])

df_processed['filter_date'] = pd.to_datetime(df_processed['Date'], format='%Y-%m-%d')

df_processed['last_date_month'] = pd.to_datetime(df_processed['filter_date'], format="%Y%m") + MonthEnd(0)

df_out = df_processed[(df_processed['last_date_month'] <= report_last_day)]
df_out['last_date_month'] = pd.to_datetime(df_out['last_date_month']).dt.date.astype(str)

del df_out['filter_date']
del df_out['Date']
df_out.rename(columns={'last_date_month': 'Date'}, inplace=True)

print('Report daate is:')
print(report_last_day)
display(df_out)

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
df_out.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_out, table_name, "overwrite")
