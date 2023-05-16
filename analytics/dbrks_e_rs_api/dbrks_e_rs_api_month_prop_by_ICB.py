# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) |2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_e_rs_api_month_prop_by_ICB.py
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

#get a list of unique dates 
list_of_dates = list(df['Report_End _Date'].unique())
#initiate output dataframe
df_output = pd.DataFrame(columns = ['E_RS Submitted Trusts', 'Acute Trusts', 'STP_Code', 'Report_End _Date'])

#for each upload date in ers data run the following code
for date in list_of_dates:
  #filter ers data for current data and append to reference data to get stp code
  df_ers = df.loc[df['Report_End _Date'] == date]
  df_ers = df_ers.merge(df_acute, on = 'ODS', how = 'left')
  df_ers = df_ers[['ODS', 'STP_Code']]
  #group by stp code and rename the column to merge with output dataframe
  df_ers = df_ers.groupby(['STP_Code']).count().reset_index()
  df_ers = df_ers.rename(columns = {'ODS' : 'E_RS Submitted Trusts'})

  #get the denominator dataframe of acute trusts and group by stp code
  df_process = df_acute.copy()
  df_process = df_process.rename(columns={'ODS':'Acute Trusts'})
  df_process = df_process.groupby(['STP_Code']).count().reset_index()
  #add in the current date as a column for each row (42)
  df_process['Report_End _Date'] = [date]*42

  #create the temp dataframe with same headers as output dataframe
  df_temp = pd.DataFrame(columns = ['E_RS Submitted Trusts', 'Acute Trusts', 'STP_Code', 'Report_End _Date'])

  #merge the numerator and denominator dataframes into df_final
  df_processed = df_temp.append(df_ers)[['E_RS Submitted Trusts', 'STP_Code']]
  df_final = pd.merge(df_process, df_processed, on = 'STP_Code', how = 'left')

  #add the df_final dataframe to df_output for each upload date
  df_output = pd.concat([df_output, df_final], ignore_index=True)

df_output = df_output.fillna(0)



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


