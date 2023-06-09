# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dscr_all_variables_cum_count.py
DESCRIPTION:
                Databricks notebook for processing code for the CQC digital social care records
USAGE:          (M323)
                ...
CONTRIBUTORS:   Abdu Nuhu and Martina Fonseca
CONTACT:        data@nhsx.nhs.uk
CREATED:        2 May. 2023
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
print(latestFolder)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_1 = df[['Location ID', 'Dormant (Y/N)','Care home?','Location Inspection Directorate','Location Primary Inspection Category','Location Local Authority','Location ONSPD CCG Code','Location ONSPD CCG','Provider ID','Provider Inspection Directorate','Provider Primary Inspection Category','Provider Postal Code', 'run_date']]

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

df_3

# COMMAND ----------

df_3.groupby(["monthly_date"]).count() # sense check

# COMMAND ----------

# Joint processing
# -------------------------------------------------------------------------
df_join = df_3.merge(df_ref_2, how ='outer', on = 'CCG_ONS_Code')
df_join.index.name = "Unique ID"
df_join = df_join.round(4)
df_join["monthly_date"] = pd.to_datetime(df_join["monthly_date"],format="%d/%m/%Y") # MF230608 . added format otherwise it was interpreting the day as month and vice-versa
df_join = df_join[df_join["Location_Inspection_Directorate"]=="Adult social care"] # MF230608. keep only Adult Social Care Primary Inspection Directorate


# COMMAND ----------

#df_join["monthly_date"][259167].month #df_3["monthly_date"]

# COMMAND ----------

# Get PIR data
# -------------------------------------------------------------------------
pir_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, pir_source_path)
file = datalake_download(CONNECTION_STRING, file_system, pir_source_path+pir_latestFolder, pir_source_file)
df_pir = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_pir # 10746 rows

# COMMAND ----------

df_pir["months"] = pd.to_datetime(df_pir["PIR submission date"]).dt.month
df_pir["year"] = pd.to_datetime(df_pir["PIR submission date"]).dt.year
df_pir["month_year"] = df_pir["months"].astype(str) + "-" + df_pir["year"].astype(str)

# columns needed from PIR (For Tab01)
df_pir_keep = df_pir[["Location ID","PIR submission date","month_year","PIR type","Use a Digital Social Care Record system?"]] 
df_pir_keep.rename(columns={"Location ID":"Location_Id"},inplace=True)

# what to keep from enriched reference data (For Tab01) (that is useful for Tableau).
df_join_keep = df_join[["Location_Id",
                        "Location_Primary_Inspection_Category",
                        "Location_Local_Authority",
                        "CCG_ONS_Code","Location_ONSPD_CCG_Name",
                        "ICB_ONS_Code","ICB_Name",
                        "Region_Code","Region_Name",
                        "Provider_ID", "monthly_date"]].copy()   

# Left join reference info to PIR (as it's a sampler)

df_tab01_sampler = df_pir_keep.merge(df_join_keep, how ='left', on ="Location_Id") # 52069 rows . 52063 ASC only
#df_join # 259168 rows . 138232 ASC only

df_tab02_patch = df_join_keep.merge(df_pir_keep, how = 'left', on='Location_Id') # MF230608 . Table02, left join PIR to reference info

# COMMAND ----------

# Create 1, 0 flag for yes and no to be use for calculation

df_tab01_sampler["yes"] = np.where(df_tab01_sampler['Use a Digital Social Care Record system?'] == "Yes", 1, 0)
df_tab01_sampler["no"] = np.where(df_tab01_sampler['Use a Digital Social Care Record system?'] == "No", 1, 0)
#df_tab01_sampler

# COMMAND ----------

# Tab02 - Create 1, 0 flag for yes and no to be use for calculation
df_tab02_patch["yes"] = np.where(df_tab02_patch['Use a Digital Social Care Record system?'] == "Yes", 1, 0)
df_tab02_patch["no"] = np.where(df_tab02_patch['Use a Digital Social Care Record system?'] == "No", 1, 0)

# COMMAND ----------

# Create spark data frame and view

spark_df = spark.createDataFrame(df_tab01_sampler)
spark_df.createOrReplaceTempView("dscr_table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Similar for Table02_patch
# MAGIC so that we have a) the position for each CQC month database of locations; b) we know the response rate (i.e. those with neither 'yes' nor 'no')

# COMMAND ----------

import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

df_tab_cumtrend = df_tab02_patch.copy()
df_tab_cumtrend["eo_monthly_date"] = df_tab_cumtrend["monthly_date"]+ pd.offsets.MonthEnd(n=0) # IMPLEMENT ME
df_tab_cumtrend = df_tab_cumtrend[(df_tab_cumtrend["PIR submission date"]<=df_tab_cumtrend["eo_monthly_date"] ) | (df_tab02_patch["PIR submission date"].isna())] # keep only submissions prior to end of month
#df_tab_cumtrend["PIR submission date"].isna().sum()
#df_tab02_patch["PIR submission date"].isna().sum()

# COMMAND ----------

df_tab_cumtrend.groupby(["eo_monthly_date","monthly_date"]).count()

# COMMAND ----------

df_tab_cumtrend_sum = df_tab_cumtrend.copy()

# COMMAND ----------

#df_tab_cumtrend_sum["cumyes"] = df_tab_cumtrend_sum.groupby(["Location_Id"]).apply(lambda x: x[x['PIR submission date'] <= x['eo_monthly_date']]['yes'].sum())
df_tab_cumtrend_sum = df_tab_cumtrend_sum.groupby(["Location_Id",
                        "Location_Primary_Inspection_Category",
                        "Location_Local_Authority",
                        "CCG_ONS_Code","Location_ONSPD_CCG_Name",
                        "ICB_ONS_Code","ICB_Name",
                        "Region_Code","Region_Name",
                        "Provider_ID", "eo_monthly_date"]).agg(YEStodate=("yes", lambda x: (x==1).sum()),
                                                           NOtodate=("no", lambda x: (x==1).sum()))
df_tab_cumtrend_sum = df_tab_cumtrend_sum.reset_index()

# COMMAND ----------

# Create necessary Tableau variable - to compute both % implementation and response rate
# https://numpy.org/doc/stable/reference/generated/numpy.select.html
conditions = [ (df_tab_cumtrend_sum["YEStodate"]>=1),
              (df_tab_cumtrend_sum["YEStodate"]<1)&(df_tab_cumtrend_sum["NOtodate"]>=1),
              (df_tab_cumtrend_sum["YEStodate"]+df_tab_cumtrend_sum["NOtodate"]==0)
               ]
PIRstatus = ["Yes","No","None"]
df_tab_cumtrend_sum["PIR_todate"] = np.select(conditions,PIRstatus)

#df_tab_cumtrend_sum["PIR_todate"]

# COMMAND ----------

#df_tab_cumtrend_sum.groupby(["eo_monthly_date","PIR_todate"]).count()
df_tab_cumtrend_sum.groupby(["eo_monthly_date"]).count()

# COMMAND ----------

max(df_pir["PIR submission date"])  # in dev only data till September 2022 is held

# COMMAND ----------

# Add run date
# ---------------------------------------------------------------------
#df_tab_cumtrend_sum["date_ran"] = datetime.now().strftime('%d-%m-%Y') 


# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
#current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
#file_contents = io.StringIO()
#df_tab_cumtrend_sum.to_csv(file_contents)
#datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Write metrics to database
# -------------------------------------------------------------------------
#write_to_sql(df_tab_cumtrend_sum, table_name, "overwrite")
