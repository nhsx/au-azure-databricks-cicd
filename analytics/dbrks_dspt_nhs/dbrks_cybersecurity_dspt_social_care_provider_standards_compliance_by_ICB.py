# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           cybersecurity_dspt_social_care_provider_standards_compliance_by_ICB.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: M394B  (Number and percent of Social Care Provider for DSPT assessment that meet or exceed the DSPT standard at ICB level)
USAGE:
                ...
CONTRIBUTORS:   Everistus Oputa
CONTACT:        NHSX.Data@england.nhs.uk
CREATED:        20 Mar 2023
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
sink_path = config_JSON['pipeline']['project']['databricks'][3]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][3]['sink_file']
table_name = config_JSON['pipeline']["staging"][3]['sink_table']

# COMMAND ----------

# Processing 
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
reference_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
reference_file = datalake_download(CONNECTION_STRING, file_system, reference_path+reference_latestFolder, reference_file)
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
(DSPT_ODS["Organisation_Code"].str.contains("RT4|RQF|RYT|0DH|0AD|0AP|0CC|0CG|0CH|0DG")==False)].reset_index(drop=True) #------ change exclusion codes for CCGs and CSUs through time. Please see SOP
DSPT_ODS_selection_3 = DSPT_ODS_selection_2[DSPT_ODS_selection_2.ODS_Organisation_Type.isin(["NON-NHS ORGANISATION"])].reset_index(drop=True)

# Creation of final dataframe with all currently open NHS Trusts which meet or exceed the DSPT standard
# --------------------------------------------------------------------------------------------------------
DSPT_ODS_selection_3 = DSPT_ODS_selection_3.rename(columns = {"Status":"Latest Status"})

DSPT_ODS_selection_4 = DSPT_ODS_selection_3[DSPT_ODS_selection_3["Latest Status"].isin(["21/22 Approaching Standards", 
                                                                                         "21/22 Standards Exceeded", 
                                                                                         "21/22 Standards Met", 
                                                                                         "21/22 Standards Not Met",
                                                                                         "Not Published"                                                                                    
                                                                                       ])].reset_index(drop=True) #------ change financial year for DSPT standard through time. Please see SOP
















# # Make ODS dataframe open and close dates datetime
# # -------------------------------------------------------------------------
# ODS_code_df['Close_Date'] = pd.to_datetime(ODS_code_df['Close_Date'], infer_datetime_format=True)
# ODS_code_df['Open_Date'] =  pd.to_datetime(ODS_code_df['Open_Date'], infer_datetime_format=True)

# # Set datefilter for org open and close dates
# # -------------------------------------------------------------------------
# close_date = datetime.strptime('2021-03-30','%Y-%m-%d') #------ change close date filter for CCGs and CSUs through time. Please see SOP
# open_date = datetime.strptime('2021-03-30','%Y-%m-%d') #------- change open date filter for CCGs and CSUs through time. Please see SOP

# # Join DSPT data with ODS table on ODS code
# # -------------------------------------------------------------------------
# # DSPT_ODS = pd.merge(ODS_code_df, DSPT_df, how='outer', left_on="Code", right_on="Code")
# ODS_code_df =ODS_code_df.reset_index(drop=True).rename(columns={"ODS_API_Role_Name": "Sector",})
# DSPT_ODS_selection =  ODS_code_df[(ODS_code_df['Close_Date'].isna() | (ODS_code_df['Close_Date'] > close_date))].reset_index(drop = True)
# DSPT_ODS_selection_1 = (DSPT_ODS_selection[DSPT_ODS_selection['Open_Date'] < open_date]).reset_index(drop = True)

# DSPT_ODS_selection_0 = DSPT_ODS_selection_1.merge(DSPT_df, how ='outer', on = 'Code')


# # Creation of final dataframe with all currently open NHS Trusts
# # -------------------------------------------------------------------------
# DSPT_ODS_selection_2 = DSPT_ODS_selection_0[ 
# # (DSPT_ODS_selection_1["Name"].str.contains("COMMISSIONING HUB")==False) &
# (DSPT_ODS_selection_0["Code"].str.contains("RT4|RQF|RYT|0DH|0AD|0AP|0CC|0CG|0CH|0DG")==False)].reset_index(drop=True) #------ change exclusion codes for CCGs and CSUs through time. Please see SOP
# DSPT_ODS_selection_3 = DSPT_ODS_selection_2[DSPT_ODS_selection_2.Sector.isin(["NON-NHS ORGANISATION"])].reset_index(drop=True)
# DSPT_ODS_selection_4 = DSPT_ODS_selection_3.rename(columns = {"Primary Sector":"PrimarySector","Status":"Latest Status"})
# # DSPT_ODS_selection_4 = DSPT_ODS_selection_3[DSPT_ODS_selection_3.PrimarySector.isin(["Social Care"])].reset_index(drop=True)

# # Creation of final dataframe with all currently open NHS Trusts which meet or exceed the DSPT standard
# # --------------------------------------------------------------------------------------------------------
# # DSPT_ODS_selection_3 = DSPT_ODS_selection_3.rename(columns = {"Status":"Latest Status"})

# DSPT_ODS_selection_5 = DSPT_ODS_selection_4[DSPT_ODS_selection_4["Latest Status"].isin(["21/22 Approaching Standards", 
#                                                                                          "21/22 Standards Exceeded", 
#                                                                                          "21/22 Standards Met", 
#                                                                                          "21/22 Standards Not Met",
#                                                                                          "Not Published"  
#                                                                                          ])].reset_index(drop=True) #------ change financial year for DSPT standard through time. Please see SOP


# COMMAND ----------


# Processing - Generating final dataframe for staging to SQL database
# -------------------------------------------------------------------------
# Generating Total_no_trusts

df1 = DSPT_ODS_selection_3[["Organisation_Code", "STP_Code"]].copy()
df1['Organisation_Code'] = df1['Organisation_Code'].astype(str)
df1 = df1.groupby(['STP_Code'], as_index=False).count()
df1['date_string'] = str(datetime.now().strftime("%Y-%m"))
df1['dspt_edition'] = "2021/2022"   #------ change DSPT edition through time. Please see SOP
df3 = df1[['date_string','dspt_edition','STP_Code', 'Organisation_Code']]
df4 = df3.rename(columns = {'date_string': 'Date','dspt_edition': 'Dspt_edition','STP_Code': 'ICB_Code','Organisation_Code':'Total_no_social_care_provider'})

# Generating Number of Trusts with a standards met or exceeded DSPT status
df1 = DSPT_ODS_selection_4[["Latest Status", "STP_Code"]].copy()
df1['Latest Status'] = df1['Latest Status'].astype(str)
df1 = df1.groupby(['Latest Status','STP_Code'], as_index=False).size()
df1['date_string'] = str(datetime.now().strftime("%Y-%m"))
df1['dspt_edition'] = "2021/2022" 
df2 = df1[['STP_Code', 'size','Latest Status']]
df3 = df2.rename(columns = {'STP_Code': 'ICB_Code','Latest Status':'status','size':'status number'})


#Joined data processing
df_join = df3.merge(df4, how ='outer', on = 'ICB_Code')
df_join_1 = df_join.rename(columns = {'Date':'Report Date','ICB_Code': 'ICB_CODE','Dspt_edition': 'Dspt_edition','Total_no_social_care_provider':'Total number of social care provider','status':'Standard status','status number':'Number of social care provider with the standard status'})
# df_join_1["Percent of Trusts with a standards met or exceeded DSPT status"] = df_join_1["Number of Trusts with the standard status"]/df_join_1["Total number of Trusts"]
df_join_1 = df_join_1.round(2)
df_join_1['Report Date'] = pd.to_datetime(df_join_1['Report Date'])
df_join_1.index.name = "Unique ID"
df_processed = df_join_1.copy()











# df1 = DSPT_ODS_selection_4[["Code", "STP_Code"]].copy()
# df1['Code'] = df1['Code'].astype(str)
# df1 = df1.groupby(['STP_Code'], as_index=False).count()
# df1['date_string'] = str(datetime.now().strftime("%Y-%m"))
# df1['dspt_edition'] = "2021/2022"   #------ change DSPT edition through time. Please see SOP
# df3 = df1[['date_string','dspt_edition','STP_Code', 'Code']]
# df4 = df3.rename(columns = {'date_string': 'Date','dspt_edition': 'Dspt_edition','STP_Code': 'ICB_Code','Code':'Total_no_social_care_provider'})

# # Generating Number of Trusts with a standards met or exceeded DSPT status
# df1 = DSPT_ODS_selection_5[["Latest Status", "STP_Code"]].copy()
# df1['Latest Status'] = df1['Latest Status'].astype(str)
# df1 = df1.groupby(['Latest Status','STP_Code'], as_index=False).size()
# df1['date_string'] = str(datetime.now().strftime("%Y-%m"))
# df1['dspt_edition'] = "2021/2022" 
# df2 = df1[['STP_Code', 'size','Latest Status']]
# df3 = df2.rename(columns = {'STP_Code': 'ICB_Code','Latest Status':'status','size':'status number'})


# #Joined data processing
# df_join = df3.merge(df4, how ='outer', on = 'ICB_Code')
# df_join_1 = df_join.rename(columns = {'Date':'Report Date','ICB_Code': 'ICB_CODE','Dspt_edition': 'Dspt_edition','Total_no_social_care_provider':'Total number of social care provider','status':'Standard status','status number':'Number of social care provider with the standard status'})
# # df_join_1["Percent of Trusts with a standards met or exceeded DSPT status"] = df_join_1["Number of Trusts with the standard status"]/df_join_1["Total number of Trusts"]
# df_join_1 = df_join_1.round(2)
# df_join_1['Report Date'] = pd.to_datetime(df_join_1['Report Date'])
# df_join_1.index.name = "Unique ID"
# df_processed = df_join_1.copy()



# COMMAND ----------

Df10

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
