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
CONTRIBUTORS:   Abdu Nuhu
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

# Joint processing
# -------------------------------------------------------------------------
df_join = df_3.merge(df_ref_2, how ='outer', on = 'CCG_ONS_Code')
df_join.index.name = "Unique ID"
df_join = df_join.round(4)
df_join["monthly_date"] = pd.to_datetime(df_join["monthly_date"])


# COMMAND ----------

# Get PIR data
# -------------------------------------------------------------------------
pir_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, pir_source_path)
file = datalake_download(CONNECTION_STRING, file_system, pir_source_path+pir_latestFolder, pir_source_file)
df_pir = pd.read_parquet(io.BytesIO(file), engine="pyarrow")


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

df_tab01_sampler = df_pir_keep.merge(df_join_keep, how ='left', on ="Location_Id")


# COMMAND ----------

# Create 1, 0 flag for yes and no to be use for calculation

df_tab01_sampler["yes"] = np.where(df_tab01_sampler['Use a Digital Social Care Record system?'] == "Yes", 1, 0)
df_tab01_sampler["no"] = np.where(df_tab01_sampler['Use a Digital Social Care Record system?'] == "No", 1, 0)


# COMMAND ----------

# Create spark data frame and view

spark_df = spark.createDataFrame(df_tab01_sampler)
spark_df.createOrReplaceTempView("dscr_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- There are duplication submission in the data for example, location_id 1-10000813008 appeared five time for PIR submission date 2022-06-21, this because data downloaded is appended to the previous months data
# MAGIC -- Use the query below to see the duplicates
# MAGIC
# MAGIC --with dup as (
# MAGIC --  select *
# MAGIC --  ,row_number() over (partition by Location_id, `PIR submission date` order by location_id, `PIR submission date`) dup_count
# MAGIC --  from dscr_table
# MAGIC --)select * from dup
# MAGIC
# MAGIC create or replace temporary view temp_view_t as 
# MAGIC
# MAGIC with dscr_all as (
# MAGIC   select *
# MAGIC   ,row_number() over (partition by Location_id, `PIR submission date` order by location_id, `PIR submission date`) rn
# MAGIC   from dscr_table
# MAGIC ),
# MAGIC
# MAGIC dscr as (
# MAGIC   select * 
# MAGIC   from dscr_all 
# MAGIC   where rn = 1        -- removed duplicates before getting unique submission
# MAGIC   and to_timestamp(`PIR submission date`) < last_day(monthly_date)
# MAGIC )
# MAGIC
# MAGIC select *
# MAGIC   ,sum(yes) over(partition by Location_Id order by monthly_date) as cum_yes 
# MAGIC   ,sum(no) over(partition by Location_Id order by monthly_date) as cum_no
# MAGIC from dscr

# COMMAND ----------

spark_df = spark.sql("select * from temp_view_t")
dscr_df = spark_df.toPandas()

# COMMAND ----------

# Pandas dataframe containing data
dscr_df

# COMMAND ----------

# Add run date
# ---------------------------------------------------------------------
#df_tab01_sampler_agg["date_ran"] = datetime.now().strftime('%d-%m-%Y') 


# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
#current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
#file_contents = io.StringIO()
#df_tab01_sampler_agg.to_csv(file_contents)
#datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Write metrics to database
# -------------------------------------------------------------------------
#write_to_sql(df_tab01_sampler_agg, table_name, "overwrite")