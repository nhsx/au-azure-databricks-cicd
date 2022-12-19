# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dscr_all_variables_month_count.py
DESCRIPTION:
                Databricks notebook with processing code for the CQC digital social care records : Monthly digital social care records mapping to icb_region
USAGE:
                ...
CONTRIBUTORS:   Everistus Oputa
CONTACT:        data@nhsx.nhs.uk
CREATED:        30 Nov. 2022
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
file_name_config = "config_dscr_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# Load PIR config file
# -------------------------------------------------------------------------
pir_file_path_config = "/config/pipelines/nhsx-au-analytics/"
pir_file_name_config = "config_digitalrecords_socialcare_dbrks.json"
pir_config_JSON = datalake_download(CONNECTION_STRING, file_system_config, pir_file_path_config, pir_file_name_config)
pir_config_JSON = json.loads(io.BytesIO(pir_config_JSON).read())


# COMMAND ----------

#Get parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_path = config_JSON['pipeline']['project']['reference_source_path']
reference_file = config_JSON['pipeline']['project']['reference_source_file']
file_system =  dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
sink_path = config_JSON['pipeline']['project']['databricks'][0]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][0]['sink_file']
table_name = config_JSON['pipeline']["staging"][0]['sink_table']

#Get parameters from PIR JSON config
# -------------------------------------------------------------------------
pir_source_path = pir_config_JSON['pipeline']['project']['source_path']
pir_source_file = pir_config_JSON['pipeline']['project']['source_file']

# COMMAND ----------

# dscr data Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_1 = df[['Location ID', 'Dormant (Y/N)','Care home?','Location Inspection Directorate','Location Primary Inspection Category','Location Local Authority','Location ONSPD CCG Code','Location ONSPD CCG','Provider ID','Provider Inspection Directorate','Provider Primary Inspection Category','Provider Postal Code','run_date']]
#df_1['run_date'] = pd.to_datetime(df_1['run_date']).dt.strftime('%Y-%m')
df_2 = df_1.drop_duplicates()
df_3 = df_2.rename(columns = {'Location ID':'Location_Id','Dormant (Y/N)':'Is_Domant','Care home?':'Is_Care_Home','Location Inspection Directorate':'Location_Inspection_Directorate','Location Primary Inspection Category':'Location_Primary_Inspection_Category','Location Local Authority':'Location_Local_Authority','Location ONSPD CCG Code':'CCG_ONS_Code','Location ONSPD CCG':'Location_ONSPD_CCG_Name','Provider ID':'Provider_ID','Provider Inspection Directorate':'Provider_Inspection_Directorate','Provider Primary Inspection Category':'Provider_Primary_Inspection_Category','Provider Postal Code':'Provider_Postal_Code','run_date':'monthly_date'})


# ref data Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system,reference_path+latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref_1 = df_ref[['CCG_ONS_Code','CCG_ODS_Code','CCG_Name','ICB_ONS_Code','ICB_Code','ICB_Name','Region_Code','Region_Name','Last_Refreshed']]
df_ref_2 = df_ref_1[~df_ref_1.duplicated(['CCG_ONS_Code', 'CCG_ODS_Code','CCG_Name','ICB_ONS_Code','ICB_Code','ICB_Name','Region_Code','Region_Name','Last_Refreshed'])].reset_index(drop = True)


# COMMAND ----------

# Joint processing
# -------------------------------------------------------------------------
df_join = df_3.merge(df_ref_2, how ='outer', on = 'CCG_ONS_Code')
df_join.index.name = "Unique ID"
df_join = df_join.round(4)
df_join["monthly_date"] = pd.to_datetime(df_join["monthly_date"])
#df_processed = df_join.copy()

# COMMAND ----------

# Get PIR data
# -------------------------------------------------------------------------
pir_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, pir_source_path)
file = datalake_download(CONNECTION_STRING, file_system, pir_source_path+pir_latestFolder, pir_source_file)
df_pir = pd.read_parquet(io.BytesIO(file), engine="pyarrow")


# COMMAND ----------

# Calculate matrics
# ------------------------------------------------------------

display(df_3.head()) # Data dowloaded from CQC website
display(df_ref_2) # Reference data from NCDR
display(df_join.head()) # Data dowloaded from CQC website joined with reference data
display(df_pir.head()) # PIR data receive monthly

# Calculated matric destination is df_processed
# ------------------------------------------------------------
# "Use a Digital Social Care Record system?"
# "PIR submission date"
#display(df_pir.head()) 

df_pir["months"] = pd.to_datetime(df_pir["PIR submission date"]).dt.month
df_pir["year"] = pd.to_datetime(df_pir["PIR submission date"]).dt.year
df_pir["month_year"] = df_pir["months"].astype(str) + "-" + df_pir["year"].astype(str)

df_period = df_pir[["month_year", "Use a Digital Social Care Record system?"]]

df_yes = df_period[df_period["Use a Digital Social Care Record system?"] == "Yes"]
df_grp_yes = df_yes.groupby("month_year",  as_index=False).agg({"Use a Digital Social Care Record system?": "count"})
df_yes_renamed = df_grp_yes.rename(columns = {"Use a Digital Social Care Record system?": "dscr_yes"})

df_grp_all = df_period.groupby("month_year",  as_index=False).agg({"Use a Digital Social Care Record system?": "count"})
df_grp_all_renamed = df_grp_all.rename(columns = {"Use a Digital Social Care Record system?": "dscr_yes_no"})

df_metric_1 = df_grp_all_renamed.merge(df_yes_renamed, how="inner", on="month_year")

df_metric_1["metric"] = (df_metric_1["dscr_yes"] / df_metric_1["dscr_yes_no"]).round(2) 

display(df_metric_1)

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
#write_to_sql(df_processed, table_name, "overwrite")
