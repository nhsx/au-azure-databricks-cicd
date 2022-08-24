# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_cybersecurity_dspt_gp_practices_standards_meet_exceed_year_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric:M76A No. and % of GP practices that are compliant with (meet or exceed) the DSPT standard (yearly
                historical) 
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli, Muhammad-Faaiz Shanawas, Martina Fonesca
CONTACT:        data@nhsx.nhs.uk
CREATED:        24 Aug. 2021
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
file_name_config = "config_dspt_gp_practices_historical_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_source_path = config_JSON['pipeline']['project']['reference_source_path']
reference_source_file = config_JSON['pipeline']['project']['reference_source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][0]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][0]['sink_file']
table_name = config_JSON['pipeline']["staging"][0]['sink_table']

# COMMAND ----------

# Ingestion and processing of numerator (DSPT status of GP practices)
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df["Snapshot_Date"] = pd.to_datetime(df["Snapshot_Date"])

# COMMAND ----------

# Ingestion and joining to reference deomintator data (NHS Digital: Number of registered GP Practices)
# ---------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_source_path)
file = datalake_download(CONNECTION_STRING, file_system, reference_source_path+latestFolder, reference_source_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# Processing - merge denominator ("ground-truth for practices") and numerator ("DSPT status"). Left join (anything not found in denominator dropped.)
# ---------------------------------------------------------------------------------------------------------------------------------------------------
df_join = df_ref.merge(df,'left',left_on=['PRACTICE_CODE','FY'],right_on=['Code','DSPT_Edition'])

# COMMAND ----------

# Processing for joined tables
# -------------------------------------------------------------------------
df_join["Edition flag_1"] = df_join["DSPT_Edition"].str[2:4] + "/" + df_join["DSPT_Edition"].str[7:] + " STANDARDS EXCEEDED"
df_join["Edition flag_2"] = df_join["DSPT_Edition"].str[2:4] + "/" + df_join["DSPT_Edition"].str[7:] + " STANDARDS MET"
df_join["Status_Raw"] = df_join["Status_Raw"].str.upper()
df_join["Status_Raw"] = df_join["Status_Raw"].replace({'STANDARDS MET (19-20)':'STANDARDS MET','NONE': 'NOT PUBLISHED'})
df_join.loc[(df_join["DSPT_Edition"]=='2018/2019') & (df_join["Status_Raw"]== 'STANDARDS MET'), "Status_Raw"] = '18/19 STANDARDS MET'
df_join.loc[(df_join["DSPT_Edition"]=='2018/2019') & (df_join["Status_Raw"]== 'STANDARDS EXCEEDED'), "Status_Raw"] = '18/19 STANDARDS EXCEEDED'
def exceed_dspt(c):
  if c['Status_Raw'] == c['Edition flag_1']:
    return 1
  else:
    return 0
def met_dspt(c):
  if c['Status_Raw'] == c['Edition flag_2']:
    return 1
  else:
    return 0
df_join['Number of GP practices that exceed the DSPT standard (historical)'] = df_join.apply(exceed_dspt, axis=1)
df_join['Number of GP practices that met the DSPT standard (historical)'] = df_join.apply(met_dspt, axis=1)
df_join["Number of GP practices that meet or exceed the DSPT standard (historical)"] = df_join['Number of GP practices that exceed the DSPT standard (historical)'] + df_join['Number of GP practices that met the DSPT standard (historical)']
df_join.rename(columns={"PRACTICE_CODE":"Practice code", "FY":"Financial year", "EXTRACT_DATE": "Date"}, inplace = True)
df_join_1 = df_join.drop(["Organisation_Name", "Status_Raw", "Edition flag_1", "Edition flag_2", "Number of GP practices that exceed the DSPT standard (historical)", 
               "Number of GP practices that met the DSPT standard (historical)","PRACTICE_NAME", "Snapshot_Date","DSPT_Edition", "Code"], axis = 1)
df_join_1.index.name = "Unique ID"
df_processed = df_join_1.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")