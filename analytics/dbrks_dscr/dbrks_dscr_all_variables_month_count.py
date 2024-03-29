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
USAGE:          (M323)
                ...
CONTRIBUTORS:   Everistus Oputa, Martina Fonseca
CONTACT:        data@nhsx.nhs.uk
CREATED:        21 Dec. 2022
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

# Load Home Care Service User config file
# -------------------------------------------------------------------------
hcsu_file_name_config = "config_home_care_user_service.json"
hcsu_config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, hcsu_file_name_config)
hcsu_config_JSON = json.loads(io.BytesIO(hcsu_config_JSON).read())

# COMMAND ----------

#Get parameters from hcsu JSON config
# -------------------------------------------------------------------------
hcsu_source_path = hcsu_config_JSON['pipeline']['proc']['sink_path']
hcsu_source_file = hcsu_config_JSON['pipeline']['proc']['sink_file']


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
df_1 = df[['Location ID', 'Dormant (Y/N)','Care home?','Location Inspection Directorate','Location Primary Inspection Category','Location Local Authority','Location ONSPD CCG Code','Location ONSPD CCG','Provider ID','Provider Inspection Directorate','Provider Primary Inspection Category','Provider Postal Code', 'run_date']]
#df_1['run_date'] = datetime.now().strftime('%d-%m-%Y')
df_2 = df_1.drop_duplicates()
df_3 = df_2.rename(columns = {'Location ID':'Location_Id','Dormant (Y/N)':'Is_Domant','Care home?':'Is_Care_Home','Location Inspection Directorate':'Location_Inspection_Directorate','Location Primary Inspection Category':'Location_Primary_Inspection_Category','Location Local Authority':'Location_Local_Authority','Location ONSPD CCG Code':'CCG_ONS_Code','Location ONSPD CCG':'Location_ONSPD_CCG_Name','Provider ID':'Provider_ID','Provider Inspection Directorate':'Provider_Inspection_Directorate','Provider Primary Inspection Category':'Provider_Primary_Inspection_Category','Provider Postal Code':'Provider_Postal_Code','run_date':'monthly_date'})


# ref data Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system,reference_path+latestFolder, reference_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref_1 = df_ref[['CCG_ONS_Code','CCG_ODS_Code','CCG_Name', 'CCG21CD','ICB_ONS_Code','ICB_Code','ICB_Name','Region_Code','Region_Name','Last_Refreshed']]
df_ref_2 = df_ref_1[~df_ref_1.duplicated(['CCG_ONS_Code', 'CCG_ODS_Code','CCG_Name', 'CCG21CD', 'ICB_ONS_Code','ICB_Code','ICB_Name','Region_Code','Region_Name','Last_Refreshed'])].reset_index(drop = True)


# COMMAND ----------

# HCSU Data Processing 
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, hcsu_source_path)
file = datalake_download(CONNECTION_STRING, file_system, hcsu_source_path+latestFolder, hcsu_source_file)
df_hcsu= pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

df_3.groupby(["Location_Id","monthly_date"],  as_index=False).agg({"Provider_ID": "count"})


# COMMAND ----------


# Joint processing
# -------------------------------------------------------------------------
df_join = df_3.merge(df_ref_2, how ='outer', left_on = 'CCG_ONS_Code', right_on = 'CCG21CD')
df_join.index.name = "Unique ID"
df_join = df_join.round(4)
df_join["monthly_date"] = pd.to_datetime(df_join["monthly_date"])
df_join=df_join[df_join["monthly_date"]==max(df_join["monthly_date"])].reset_index() # MF: keep only latest months' CQC?
#df_processed = df_join.copy()



# COMMAND ----------

# Get PIR data
# -------------------------------------------------------------------------
pir_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, pir_source_path)
file = datalake_download(CONNECTION_STRING, file_system, pir_source_path+pir_latestFolder, pir_source_file)
df_pir = pd.read_parquet(io.BytesIO(file), engine="pyarrow")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tab01-granular
# MAGIC Calculation of metric Table1 - "monthly sampler", i.e. focusses on PIR responses (Tab01)
# MAGIC - Does not track full Universe of locations* (given by df_join), but does bring in some useful fields from df_join (e.g. ICB, provider ID, if need be)
# MAGIC - Does intend to keep individual responses (e.g. sometimes same location has two PIR submissions in a same month given different PIR type or re-submission)
# MAGIC
# MAGIC * this is intended for Table2 - "patch" . i.e. it should have at least a row per CQC ASC provider and then where available add in PIR info. This can help track completion

# COMMAND ----------


df_join['Last_Refreshed'] = pd.to_datetime(df_join['Last_Refreshed'])
df_join_keep = df_join[df_join["Last_Refreshed"]==max(df_join["Last_Refreshed"])]

df_pir["months"] = pd.to_datetime(df_pir["PIR submission date"]).dt.month
df_pir["year"] = pd.to_datetime(df_pir["PIR submission date"]).dt.year
df_pir["month_year"] = df_pir["months"].astype(str) + "-" + df_pir["year"].astype(str)

# columns needed from PIR (For Tab01)
df_pir_keep = df_pir[["Location ID","PIR submission date","month_year","PIR type","Use a Digital Social Care Record system?"]] 
df_pir_keep.rename(columns={"Location ID":"Location_Id"},inplace=True)

# what to keep from enriched reference data (For Tab01) (that is useful for Tableau).
df_join_keep = df_join[df_join["Last_Refreshed"]==max(df_join["Last_Refreshed"])][["Location_Id",
                        "Location_Primary_Inspection_Category",
                        "Location_Local_Authority",
                        "CCG_ONS_Code_y","Location_ONSPD_CCG_Name",
                        "ICB_ONS_Code","ICB_Name",
                        "Region_Code","Region_Name",
                        "Provider_ID", "monthly_date", "Is_Domant"]].copy()   

# Left join reference info to PIR (as it's a sampler)
df_join_keep = df_join_keep.rename(columns={'CCG_ONS_Code_y':'CCG_ONS_Code'})

df_tab01_sampler = df_pir_keep.merge(df_join_keep, how ='left', on ="Location_Id")


# COMMAND ----------

## Add some auxiliaries to indicate more than one submission in a month. Bear also in mind that some (a minority) may be submitting more than once yearly
# https://stackoverflow.com/questions/59486029/python-how-to-groupby-and-count-without-aggregating-the-dataframe
aux_group =df_tab01_sampler.groupby(['Location_Id','month_year'],as_index=False)

df_tab01_sampler['PIRm_n']=aux_group['Use a Digital Social Care Record system?'].transform('count')  # this indicates for the given month-year and same location how many responses

# COMMAND ----------


aux_PIRm_anyYES= aux_group['Use a Digital Social Care Record system?'].apply(lambda x: (x=="Yes").sum()).rename(columns={"Use a Digital Social Care Record system?":"PIRm_anyYES"})#['Use a Digital Social Care Record system?']

aux_PIRm_anyYES["PIRm_anyYES"] = aux_PIRm_anyYES["PIRm_anyYES"]>0 # this indicates for the given month-year and same location if any response was "Yes" and attributes that location-month a TRUE if so.

df_tab01_sampler = df_tab01_sampler.merge(aux_PIRm_anyYES, how ='left', on =["Location_Id","month_year"])  # join the PIRm_anyYES var to the main


# COMMAND ----------

aux=df_tab01_sampler.sort_values(by=['PIRm_n','Location_Id']) # sense check . #1-6805237133 as example as duplicate with mix in-month
aux.iloc[-20:,];

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tab01-Agg01
# MAGIC Some level of aggregation of Tab01 if need be (NOTE: this considers same-location submissions for a same PIR-month as valid to be counted as separate for the metric)
# MAGIC Information of what is duplicate will get lost here since we collapse the location id dimension. We assume this is desired/acceptable in order to track submissions and have more granularity by PIR type.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge Capacity Tracker Data 
# MAGIC Get Number of Users for domiciliary and residential care and join on CQC ID, PIR Type and month-year

# COMMAND ----------


from pandas.tseries.offsets import DateOffset

#rename is domcare to community or residential and put in pir_type column
df_hcsu['PIR type'] = df_hcsu['IsDomcare'].replace(1, 'Community')

df_hcsu['PIR type'] = df_hcsu['PIR type'].replace(0, 'Residential')

#subtract one month from the month_year so that we join it to the previous months cqc data
df_hcsu['month_year'] = df_hcsu['Date'] + DateOffset(months=1)

#format the date to match the cqc data
df_hcsu['month_year'] = df_hcsu['month_year'].dt.strftime('%m-%Y')

#reformat the serviceuser count to int
df_hcsu['ServiceUserCount'] = df_hcsu['ServiceUserCount'].astype(int)

#drop unnecessary columns for merging
df_hcsu = df_hcsu[['CqcId', 'ServiceUserCount', 'PIR type', 'month_year']]

#rename service user count column
df_hcsu = df_hcsu.rename(columns = {'ServiceUserCount':'Number_Of_People_Served'})

# COMMAND ----------


#rename location id column to cqcid to merge with hcsu data
df_tab01_sampler = df_tab01_sampler.rename(columns = {'Location_Id':'CqcId'})

#format the date to datetime
df_tab01_sampler['month_year'] = pd.to_datetime(df_tab01_sampler['month_year']).dt.strftime('%m-%Y')

#df_tab01_sampler = df_tab01_sampler.merge(df_hcsu, on = ['CqcId', 'month_year', 'PIR type'], how = 'left')
#display(df_tab01_sampler)

# COMMAND ----------


df_tab01_sampler_agg = df_tab01_sampler.groupby([
                                                "month_year",
                                                 "PIR type",
                                                 "Location_Primary_Inspection_Category",
                                                 "Location_Local_Authority",
                                                 "CCG_ONS_Code","Location_ONSPD_CCG_Name",
                                                 "ICB_ONS_Code","ICB_Name","Is_Domant", #"Number_Of_People_Served",
                                                 "Region_Code","Region_Name"]).agg(PIR_YES=("Use a Digital Social Care Record system?", lambda x: (x=="Yes").sum()),
                                                                                   PIR_NO=("Use a Digital Social Care Record system?", lambda x: (x=="No").sum()),
                                                                                   PIR_COUNT=("Use a Digital Social Care Record system?", "count")) # done dif from yes and no but should add up. Change to Yes+No if better

df_tab01_sampler_agg = df_tab01_sampler_agg.reset_index()

df_tab01_sampler_agg['PIR_PERCENTAGE']=df_tab01_sampler_agg['PIR_YES']/(df_tab01_sampler_agg['PIR_YES']+df_tab01_sampler_agg['PIR_NO'])

# COMMAND ----------

# Add run date
# ---------------------------------------------------------------------
df_tab01_sampler_agg["date_ran"] = datetime.now().strftime('%d-%m-%Y') 
display(df_tab01_sampler_agg)

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
df_tab01_sampler_agg.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Write metrics to database
# -------------------------------------------------------------------------
write_to_sql(df_tab01_sampler_agg, table_name, "overwrite")
