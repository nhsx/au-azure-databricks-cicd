# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           cybersecurity_dspt_nhs_trusts_standards_compliance_by_ICB.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: M394  (Number and percent of Trusts registered for DSPT assessment that meet or exceed the DSPT standard at ICB level)
USAGE:
                ...
CONTRIBUTORS:   Everistus Oputa
CONTACT:        NHSX.Data@england.nhs.uk
CREATED:        13 Mar 2023
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
import calendar

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
file_name_config = "config_dspt_nhs_dbrks.json"
file_system_config =  dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_path = config_JSON['pipeline']['project']['reference_path2']
reference_file = config_JSON['pipeline']['project']['reference_file2']
file_system =  dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
sink_path = config_JSON['pipeline']['project']['databricks'][2]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][2]['sink_file']
table_name = config_JSON['pipeline']["staging"][2]['sink_table']

# COMMAND ----------

reference_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
reference_file = datalake_download(CONNECTION_STRING, file_system, reference_path+reference_latestFolder, reference_file)

# COMMAND ----------

def datalake_list_folders(CONNECTION_STRING, file_system, source_path):
  try:
      service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
      file_system_client = service_client.get_file_system_client(file_system=file_system)
      pathlist = list(file_system_client.get_paths(source_path))
      folders = []
      # remove file_path and source_file from list
      for path in pathlist:
        folders.append(path.name.replace(source_path.strip("/"), "").lstrip("/").rsplit("/", 1)[0])
        folders.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))
      
      return folders
  except Exception as e:
      print(e)

# COMMAND ----------

def get_latest_dates(dates):
    # Convert the list of dates to datetime objects
    datetime_dates = [datetime.strptime(date, "%Y-%m-%d") for date in dates]

    # Create a dictionary to store the latest date for each month
    latest_dates = {}
    
    # Iterate over the datetime_dates and update the latest_dates dictionary
    for date in datetime_dates:
        month = date.month
        if month not in latest_dates or date > latest_dates[month]:
            latest_dates[month] = date

    # Return the list of latest dates
    return list(latest_dates.values())

# COMMAND ----------

all_folders = datalake_list_folders(CONNECTION_STRING, file_system, source_path)
latest_dates = get_latest_dates(all_folders)
latest_folders = []
for i in latest_dates:
  latest_folders.append(datetime.strftime(i, "%Y-%m-%d"))
#latest_folders


# COMMAND ----------

# Processing 
# -------------------------------------------------------------------------
df_processed = pd.DataFrame(columns = ['ICB_Code', 'Latest Status', 'Number of Trusts with standard status', 'Total number of Trusts', 'Snapshot Date'])
for folder in latest_folders:
  latestFolder = folder + '/'
  file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
  DSPT_df = pd.read_csv(io.BytesIO(file))
  ODS_code_df = pd.read_parquet(io.BytesIO(reference_file), engine="pyarrow")

  # Make all ODS codes in DSPT dataframe capital
  # -------------------------------------------------------------------------
  DSPT_df['Code'] = DSPT_df['Code'].str.upper()
  DSPT_df = DSPT_df.rename(columns = {'Code': 'Organisation_Code'})

  # Join DSPT data with ODS table on ODS code
  # -------------------------------------------------------------------------
  DSPT_ODS = ODS_code_df.merge(DSPT_df, how ='outer', on = 'Organisation_Code')

  # Creation of final dataframe with all currently open NHS Trusts
  # -------------------------------------------------------------------------
  DSPT_ODS_selection_2 = DSPT_ODS[ 
  (DSPT_ODS["Organisation_Code"].str.contains("RT4|RQF|RYT|0DH|0AD|0AP|0CC|0CG|0CH|0DG|RVY")==False)].reset_index(drop=True) #------ change exclusion codes for CCGs and CSUs through time. Please see SOP
  DSPT_ODS_selection_3 = DSPT_ODS_selection_2[DSPT_ODS_selection_2.ODS_Organisation_Type.isin(["NHS TRUST", "CARE TRUST"])].reset_index(drop=True)

  # Creation of final dataframe with all currently open NHS Trusts which meet or exceed the DSPT standard
  # --------------------------------------------------------------------------------------------------------
  DSPT_ODS_selection_3 = DSPT_ODS_selection_3.rename(columns = {"Status":"Latest Status"})


  # Processing - Generating final dataframe for staging to SQL database
  # -------------------------------------------------------------------------
  # Generating Total_no_trusts

  #2019/2020
  df1 = DSPT_ODS_selection_3[["Organisation_Code", "STP_Code", 'Latest Status']].copy()
  list_of_statuses1 = ["19/20 Approaching Standards", 
                        "19/20 Standards Exceeded", 
                        "19/20 Standards Met", 
                        "19/20 Standards Not Met",
                        "20/21 Approaching Standards", 
                        "20/21 Standards Exceeded", 
                        "20/21 Standards Met", 
                        "20/21 Standards Not Met",
                        "21/22 Approaching Standards", 
                        "21/22 Standards Exceeded", 
                        "21/22 Standards Met", 
                        "21/22 Standards Not Met",
                        "22/23 Approaching Standards", 
                        "22/23 Standards Exceeded", 
                        "22/23 Standards Met", 
                        "22/23 Standards Not Met",
                        'Not Published']
  
#if the date is not within the financial year remove outdated statuses from the list
  max_date = pd.to_datetime(DSPT_ODS_selection_3['Date Of Publication'].max()).strftime('%Y-%m-%m')
  if max_date > '2021-07-01':
    list_of_statuses1 = list_of_statuses1[4:]   
  elif max_date > '2022-07-01':
    list_of_statuses1 = llist_of_statuses1[8:]
  elif max_date > '2023-07-01':
    list_of_statuses1 = llist_of_statuses1[12:]
    
  df1 = df1[df1['Latest Status'].isin(list_of_statuses1)]

  df1['Organisation_Code'] = df1['Organisation_Code'].astype(str)
  df1a = df1.groupby(['STP_Code'], as_index=False).count() 
  df1a.drop(['Latest Status'], axis = 1, inplace = True)
  df1a = df1a.rename(columns = {'Organisation_Code':'Total number of Trusts'})
  df1b = df1.groupby(['STP_Code', 'Latest Status'], as_index=False).size()  
  df1b = df1b.rename(columns = {'size':'Number of Trusts with standard status'})
  df1 = pd.merge(df1a, df1b, on = ['STP_Code'], how = 'left')




  #Joined data processing
  df_join_1 = df1.rename(columns = {'STP_Code':'ICB_Code'})
  df_join_1.index.name = "Unique ID"
  df_join_1['Snapshot Date'] = folder
  df_processed = pd.concat([df_processed, df_join_1], ignore_index=True) 


# COMMAND ----------

today = str(datetime.today().strftime('%Y-%m-%d'))

today_year = today[0:4]
today_month = int(today[5:7])
report_month = today_month - 1
report_date_str = today_year + '-' + str(report_month) + '-01'

report_date = datetime.strptime(report_date_str, '%Y-%m-%d')
report_last_day = report_date.replace(day = calendar.monthrange(report_date.year, report_date.month)[1])

df_processed['filter_date'] = pd.to_datetime(df_processed['Snapshot Date'], format='%Y-%m-%d')

df_out = df_processed[(df_processed['filter_date'] <= report_last_day)]
del df_out['filter_date']

print('Report date is:')
print(report_last_day)

display(df_out)

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_out.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_out, table_name, "overwrite")
