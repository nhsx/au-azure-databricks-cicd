# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dscr_all_variables_collated_count.py
DESCRIPTION:
                Databricks notebook with processing code for the CQC digital social care records metric M324-M326 : Collated CQC database with info on a) digital social care records mapping and b) icb_region.
USAGE:          (M324, M325, M326, M334)
                ...
CONTRIBUTORS:   Everistus Oputa, Martina Fonseca
CONTACT:        data@nhsx.nhs.uk
CREATED:        17 Feb. 2023
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

# Load Home Care Service User config file
# -------------------------------------------------------------------------
hcsu_file_name_config = "config_home_care_user_service.json"
hcsu_config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, hcsu_file_name_config)
hcsu_config_JSON = json.loads(io.BytesIO(hcsu_config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_path = config_JSON['pipeline']['project']['reference_source_path']
reference_file = config_JSON['pipeline']['project']['reference_source_file']
file_system =  dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
sink_path = config_JSON['pipeline']['project']['databricks'][1]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][1]['sink_file']
table_name = config_JSON['pipeline']["staging"][1]['sink_table']

#Get parameters from PIR JSON config
# -------------------------------------------------------------------------
pir_source_path = pir_config_JSON['pipeline']['project']['source_path']
pir_source_file = pir_config_JSON['pipeline']['project']['source_file']

#Get parameters from hcsu JSON config
# -------------------------------------------------------------------------
hcsu_source_path = hcsu_config_JSON['pipeline']['proc']['sink_path']
hcsu_source_file = hcsu_config_JSON['pipeline']['proc']['sink_file']

# COMMAND ----------

# dscr data Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_1 = df[['Location ID', 'Dormant (Y/N)','Care home?', 'Care homes beds', 'Location Inspection Directorate','Location Primary Inspection Category','Location Local Authority','Location ONSPD CCG Code','Location ONSPD CCG','Provider ID','Provider Inspection Directorate','Provider Primary Inspection Category','Provider Postal Code','run_date']]

df_2 = df_1.drop_duplicates()
df_3 = df_2.rename(columns = {'Location ID':'Location_Id','Dormant (Y/N)':'Is_Domant','Care home?':'Is_Care_Home', 'Care homes beds':'Care_Home_Beds', 'Location Inspection Directorate':'Location_Inspection_Directorate','Location Primary Inspection Category':'Location_Primary_Inspection_Category','Location Local Authority':'Location_Local_Authority','Location ONSPD CCG Code':'CCG_ONS_Code','Location ONSPD CCG':'Location_ONSPD_CCG_Name','Provider ID':'Provider_ID','Provider Inspection Directorate':'Provider_Inspection_Directorate','Provider Primary Inspection Category':'Provider_Primary_Inspection_Category','Provider Postal Code':'Provider_Postal_Code','run_date':'monthly_date'})


# ref data Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system,reference_path+latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref_1 = df_ref[['CCG_ONS_Code','CCG_ODS_Code','CCG_Name', 'CCG21CD', 'ICB_ONS_Code','ICB_Code','ICB_Name','Region_Code','Region_Name','Last_Refreshed']]
df_ref_2 = df_ref_1[~df_ref_1.duplicated(['CCG_ONS_Code', 'CCG_ODS_Code','CCG_Name', 'CCG21CD', 'ICB_ONS_Code','ICB_Code','ICB_Name','Region_Code','Region_Name','Last_Refreshed'])].reset_index(drop = True)

# HCSU Data Processing 
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, hcsu_source_path)
file = datalake_download(CONNECTION_STRING, file_system, hcsu_source_path+latestFolder, hcsu_source_file)
df_hcsu= pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

datalake_latestFolder(CONNECTION_STRING, file_system, 'proc/sources/digital_socialcare/manual_upload/home_care_user_service/historical')

# COMMAND ----------

df_hcsu.rename(columns={'CqcId': 'Location_Id'}, inplace=True)

# COMMAND ----------

df_3.groupby(["Location_Id","monthly_date"],  as_index=False).agg({"Provider_ID": "count"})

# COMMAND ----------

# Joint processing
# -------------------------------------------------------------------------
df_join = df_3.merge(df_ref_2, how ='outer', left_on = 'CCG_ONS_Code', right_on = 'CCG21CD')
df_join.index.name = "Unique ID"
df_join = df_join.round(4)

#df_join["monthly_date"] = pd.to_datetime(df_join["monthly_date"])
#df_join=df_join[df_join["monthly_date"]==max(df_join["monthly_date"])].reset_index() # MF: keep only latest months' CQC?

df_join["monthly_date"] = pd.to_datetime(df_join["monthly_date"], format='%d/%m/%Y')
df_join = df_join[df_join["Location_Inspection_Directorate"]=="Adult social care"] # keep only Adult Social Care Primary Inspection Directorate
#df_processed = df_join.copy()

# COMMAND ----------

# Get PIR data
# -------------------------------------------------------------------------
pir_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, pir_source_path)
file = datalake_download(CONNECTION_STRING, file_system, pir_source_path+pir_latestFolder, pir_source_file)
df_pir = pd.read_parquet(io.BytesIO(file), engine="pyarrow")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tab02 ("path" / "collated")
# MAGIC Calculation of metric Table2 - "collated", i.e. focusses on CQC universe and which submitted PIR responses
# MAGIC - Does track full Universe of current locations, i.e. those in latest CQC file (given by df_join)
# MAGIC - Does intend to keep individual responses but only so far as an individual CQC location level, i.e.:
# MAGIC
# MAGIC  a) A step is done to only keep the most recent PIR submission per location and per PIR type (residential / community) - so old submissions or error duplicates are removed
# MAGIC  
# MAGIC  b) Since the aim is to simplify on the view of whether a location has a DSCR at all, the PIR DSCR metric is simplified to capture "Does any PIR type from this location report having a DSCR?". If any of the latest PIR type forms indicates "Yes", one of these is retained against the CQC location.
# MAGIC  
# MAGIC  caveats:
# MAGIC  - since the latest CQC file is used, organisations that have since closed won't be considered in terms of their PIR responses.

# COMMAND ----------

df_join['Last_Refreshed'] = pd.to_datetime(df_join['Last_Refreshed'])
df_join_keep = df_join[df_join["Last_Refreshed"]==max(df_join["Last_Refreshed"])]

df_pir["months"] = pd.to_datetime(df_pir["PIR submission date"]).dt.month
df_pir["year"] = pd.to_datetime(df_pir["PIR submission date"]).dt.year
df_pir["month_year"] = df_pir["months"].astype(str) + "-" + df_pir["year"].astype(str)

# columns needed from PIR (For Tab01)
df_pir_keep = df_pir[["Location ID","PIR submission date","month_year","PIR type","Use a Digital Social Care Record system?"]] 
df_pir_keep.rename(columns={"Location ID":"Location_Id"},inplace=True)

## Add some auxiliaries to indicate more than one submission in a month. Bear also in mind that some (a minority) may be submitting more than once yearly
# https://stackoverflow.com/questions/59486029/python-how-to-groupby-and-count-without-aggregating-the-dataframe
aux_group =df_pir_keep.groupby(['Location_Id'],as_index=False)

df_pir_keep['PIRm_n']=aux_group['Use a Digital Social Care Record system?'].transform('count')  # this indicates for the same location how many responses

# what to keep from enriched reference data (For Tab02) (that is useful for Tableau).
df_join_keep = df_join[df_join["Last_Refreshed"]==max(df_join["Last_Refreshed"])][["Location_Id",
                        "Location_Primary_Inspection_Category",
                        "Location_Local_Authority",
                        "CCG_ONS_Code_y", "Location_ONSPD_CCG_Name",
                        "ICB_ONS_Code","ICB_Name",
                        "Region_Code","Region_Name",
                        "Provider_ID", "monthly_date", 'Is_Care_Home', 'Care_Home_Beds', 'Is_Domant']].copy()   

df_join_keep = df_join_keep.rename(columns = {'CCG_ONS_Code_y':'CCG_ONS_Code'})


# COMMAND ----------

display(df_join_keep)

# COMMAND ----------

# Filtering for domcare

df_hcsu = df_hcsu.loc[(df_hcsu['IsActive'] == 1) & (df_hcsu['IsDomcare'] == 1)]
print("Lenth of DF after filtering only for IsActive - 1 and IsDomcare - 1:", len(df_hcsu))

# COMMAND ----------

# Merging data from Home Care Service User File
df_hcsu['join_date'] = df_hcsu['Date'] + np.timedelta64(1, 'M')
df_hcsu['join_date'] = df_hcsu['join_date'].dt.strftime('%Y-%m')
df_join_keep['join_date'] = df_join_keep['monthly_date'].dt.strftime('%Y-%m')
df_merged_hcsu_df_join_keep = pd.merge(df_join_keep, df_hcsu, on=['Location_Id', 'join_date'] ,how="left")


# COMMAND ----------

df_merged_hcsu_df_join_keep = df_merged_hcsu_df_join_keep.drop(['Date', 'IsActive', 'IsDomcare', 'join_date'], axis = 1)
display(df_merged_hcsu_df_join_keep)

# COMMAND ----------

df_pir_keep[df_pir_keep["PIRm_n"]>1].sort_values("Location_Id")

# COMMAND ----------

# For PIR, keep only most recent submission per location x type (remove earlier responses)

df_pir_keep_unit = df_pir_keep.sort_values('PIR submission date').groupby(["Location_Id","PIR type"]).tail(1)

# COMMAND ----------

df_pir_keep_unit[df_pir_keep_unit["PIRm_n"]>1].sort_values("Location_Id")

# COMMAND ----------

df_pir_keep.sort_values(['PIR type','Use a Digital Social Care Record system?'])

# COMMAND ----------

# For PIR, keep only one submission per location - if any PIR type says yes, conserve a 'yes' submission (the sort ensures that the tail would capture a yes, if present at all)

df_pir_keep_unit2 = df_pir_keep_unit.sort_values('Use a Digital Social Care Record system?').groupby(["Location_Id"]).tail(1)


# COMMAND ----------

# Left join PIR to reference info (since it means to collate)

df_tab02_patch = df_merged_hcsu_df_join_keep.merge(df_pir_keep_unit2, how ='left', on ="Location_Id")


# COMMAND ----------

# Add run date
# ---------------------------------------------------------------------
df_tab02_patch["run_date"] = df_join_keep["monthly_date"]


# COMMAND ----------

display(df_tab02_patch)

# COMMAND ----------

sink_path

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
df_tab02_patch.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Write metrics to database
# -------------------------------------------------------------------------
write_to_sql(df_tab02_patch, table_name, "overwrite")

# COMMAND ----------


