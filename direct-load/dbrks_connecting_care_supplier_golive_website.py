# Databricks notebook source
#!/usr/bin python3

# --------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
FILE:           dbrks_connecting_care_supplier_golive_website.py
DESCRIPTION:
                Databricks notebook for oading connecting care data for go live and websites, input is expected to be excel file
              
USAGE:
                ...
CONTRIBUTORS:   Abdu Nuhu
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        25 June. 2024
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* xlrd openpyxl python-dateutil fastparquet

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
import json
import fastparquet

# 3rd party:
import pandas as pd
import numpy as np
import requests
from pathlib import Path
from urllib import request as urlreq
from bs4 import BeautifulSoup
from azure.storage.filedatalake import DataLakeServiceClient
from dateutil.relativedelta import relativedelta

# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONNECTION_STRING")


# COMMAND ----------

# MAGIC %run /Shared/databricks/au-azure-databricks-cicd/functions/dbrks_helper_functions

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/direct-load/"
file_name_config = "config_connecting_care_golive_website.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
new_source_path = config_JSON['pipeline']['raw']['snapshot_source_path']
sink_path = config_JSON['pipeline']['proc']['sink_path']
sink_file = config_JSON['pipeline']['proc']['sink_file']
table_name = config_JSON['pipeline']["staging"][0]['sink_table']

print(new_source_path)
print(sink_path)
print(sink_file)
print(table_name)


# COMMAND ----------

# Pull new dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
print(new_source_path)
print(latestFolder)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
file_name_list = [file for file in file_name_list if '.xlsx' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)

  suppliers_df = pd.read_excel(io.BytesIO(new_dataset), sheet_name = "Suppliers", header = 0, engine='openpyxl') 
  go_live_df = pd.read_excel(io.BytesIO(new_dataset), sheet_name = "Go live date", header = 0, engine='openpyxl') 
  website_df = pd.read_excel(io.BytesIO(new_dataset), sheet_name = "Website", header = 0, engine='openpyxl') 
  

# COMMAND ----------

# Eliminate columns that are not needed
supplier_col = ["ICB", "System Supplier"]
sys_supplier_df = suppliers_df[supplier_col]
go_live_df.rename(columns = {"When did your ShCR go live across your ICB? (the initial go-live date if you have re-procured)": "GO_Live_Date"}, inplace = True)


# COMMAND ----------

# Check number of ICB in each tab
supplier_icb_count = sys_supplier_df["ICB"].count()
go_live_icb_count = go_live_df["ICB"].count()
website_icb_count = website_df["ICB"].count()

print(f"Number of ICB in supplier tab is: {supplier_icb_count}")
print(f"Number of ICB in go live tab is: {go_live_icb_count}")
print(f"Number of ICB in website tab is: {website_icb_count}")


# COMMAND ----------

# Link all three tabs to one dataframe.    new_dataframe["GO_Live_Date"] = pd.to_datetime(new_dataframe["GO_Live_Date"])

# Convert the columns to the same type
go_live_df["ICB"] = go_live_df["ICB"].astype(str)
website_df["ICB"] = website_df["ICB"].astype(str)
sys_supplier_df["ICB"] = sys_supplier_df["ICB"].astype(str)

# Merge the dataframes
supp_golive_web_df = go_live_df.merge(website_df, on="ICB", how='left').merge(sys_supplier_df, on="ICB", how='left')


# COMMAND ----------

# Get reference data
ref_df = read_sql_server_table("stp_code_mapping_snapshot")
pandas_icb_df = ref_df.toPandas()
icb_df = pandas_icb_df[["ODS_STP_code", "STP_name"]]
icb_df.rename(columns = {"STP_name": "ICB"}, inplace = True)


# COMMAND ----------

# Link data with reference data
supp_golive_web_df = supp_golive_web_df.merge(icb_df, on="ICB", how='left')

print(supp_golive_web_df.columns)
supp_golive_web_df

# COMMAND ----------

#Upload hsitorical appended data to datalake
# -----------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
new_dataframe.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Write metrics to database
# -------------------------------------------------------------------------
pd.DataFrame.iteritems = pd.DataFrame.items
write_to_sql(new_dataframe, table_name, "overwrite")

