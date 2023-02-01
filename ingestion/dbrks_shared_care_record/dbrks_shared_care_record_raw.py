# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_shared_care_record_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analyticus unit metrics within the Shared Care Record topic.
                
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        08th of Sept. 2022
VERSION:        0.0.2
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake  beautifulsoup4 numpy urllib3 lxml dateparser regex openpyxl pyarrow==5.0.*

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
import json
import regex as re

# 3rd party:
import pandas as pd
import numpy as np
import openpyxl
from pathlib import Path
from azure.storage.filedatalake import DataLakeServiceClient
from openpyxl import load_workbook

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
file_name_config = "config_shared_care_record_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['raw']['source_path']
sink_path = config_JSON['pipeline']['raw']['sink_path']
historical_source_path = config_JSON['pipeline']['raw']['sink_path']

# COMMAND ----------

# Functions required for data ingestion and processing 
# ----------------------------------------------------

def datalake_listDirectory(CONNECTION_STRING, file_system, source_path):
    try:
        service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
        file_system_client = service_client.get_file_system_client(file_system=file_system)
        paths = file_system_client.get_paths(path=source_path)
        directory = []
        for path in paths:
            path.name = path.name.replace(source_path, '')
            directory.append(path.name)
    except Exception as e:
        print(e)
    return directory, paths
  

def get_sheetnames_xlsx(filepath):
    wb = load_workbook(filepath, read_only=True, keep_links=False)
    return wb.sheetnames

# COMMAND ----------

# Get LatestFolder and list of all files in latest folder
# --------------------------------------------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
directory, paths = datalake_listDirectory(CONNECTION_STRING, file_system, source_path+latestFolder)

# COMMAND ----------

# Ingestion and processing of data from individual excel sheets.
# -------------------------------------------------------------

#initialize dataframes
stp_df = pd.DataFrame()
trust_df = pd.DataFrame()
pcn_df = pd.DataFrame()
la_df = pd.DataFrame()
other_df = pd.DataFrame()
community_df = pd.DataFrame()

#loop through each submitted file in the landing area. FOr each file its goes through the various sheets and adds them to an output 
for filename in directory:
    file = datalake_download(CONNECTION_STRING, file_system, source_path + latestFolder, filename)
    
    #Read current file inot an iobytes object, read that object and get list of sheet names
    sheets = get_sheetnames_xlsx(io.BytesIO(file))

    ### STP CALCULATIONS ###
    #list comprehension to get sheets with ICB in the name from list of all sheets - presumably it should only ever be 1
    sheet_name = [sheet for sheet in sheets if sheet.startswith("ICB")]
    
    #Read current sheet. The output is a dictionary. Top level is ICB Sheets should be just 1, next level is each column on the sheet
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine="openpyxl")
    for key in xls_file:
        #drop unnamed columns
        xls_file[key].drop(list(xls_file[key].filter(regex="Unnamed:")), axis=1, inplace=True)
        #remove empty rows if column 1 is empty
        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month\nsee guidance Ref 2"].isnull()]
        #rename columns based on order
        xls_file[key].rename(
            columns={
                list(xls_file[key])[0]: "For Month",
                list(xls_file[key])[1]: "ODS STP Code",
                list(xls_file[key])[2]: "STP Name",
                list(xls_file[key])[3]: "ICS Name (if applicable)",
                list(xls_file[key])[4]: "ShCR Programme Name",
                list(xls_file[key])[5]: "Name of ShCR System",
                ###Extra columns
                list(xls_file[key])[10]: "Access to Advanced (EoL) Care Plans",
                list(xls_file[key])[11]: "Number of users with access to the ShCR",
                list(xls_file[key])[12]: "Number of ShCR views in the past month",
                list(xls_file[key])[13]: "Number of unique user ShCR views in the past month",
                list(xls_file[key])[14]: "Completed by (email)",
                list(xls_file[key])[15]: "Date completed",
            },
            inplace=True,
        )
        
        # get excel file metadata
        STP_code = xls_file[key]["ODS STP Code"].unique()[0]  # get stp code for all sheets
        STP_name = xls_file[key]["STP Name"].unique()[0]  # get stp name for all sheets
        ICS_name = xls_file[key]["ICS Name (if applicable)"].unique()[0]  # get ics name for all sheets
          
        #For numeric fields, fill in blanks with zeros. Replace any non numeric entries with zero.
        xls_file[key]["Number of users with access to the ShCR"] = pd.to_numeric(xls_file[key]["Number of users with access to the ShCR"], errors='coerce').fillna(0).astype(int)
        xls_file[key]["Number of ShCR views in the past month"] = pd.to_numeric(xls_file[key]["Number of ShCR views in the past month"], errors='coerce').fillna(0).astype(int)
        xls_file[key]["Number of unique user ShCR views in the past month"] = pd.to_numeric(xls_file[key]["Number of unique user ShCR views in the past month"], errors='coerce').fillna(0).astype(int)

        # append results to dataframe dataframe
        stp_df = stp_df.append(xls_file[key], ignore_index=True)

    ### TRUST CALCULATIONS ###
    sheet_name = [sheet for sheet in sheets if sheet.startswith("Trust")]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine="openpyxl")
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex="Unnamed:")), axis=1, inplace=True)

        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        xls_file[key].rename(
            columns={
                list(xls_file[key])[0]: "For Month",
                list(xls_file[key])[1]: "ODS Trust Code",
                list(xls_file[key])[2]: "Trust Name",
                list(xls_file[key])[3]: "Partner Organisation connected to ShCR?",
                #extra column
                list(xls_file[key])[5]: "Partner Organisation plans to be connected by March 2023?",
            },
            inplace=True,
        )
        xls_file[key].insert(1, "ODS STP Code", STP_code, False)
        xls_file[key].insert(2, "STP Name", STP_name, False)
        xls_file[key].insert(3, "ICS Name (if applicable)", ICS_name, False)
        xls_file[key]["Partner Organisation connected to ShCR?"] = xls_file[key]["Partner Organisation connected to ShCR?"].map({"Connected": 1, "Not Connected": 0, "Please select": 0}).fillna(0).astype(int)     
        xls_file[key]["Partner Organisation plans to be connected by March 2023?"] = xls_file[key]["Partner Organisation plans to be connected by March 2023?"].map({"yes": 1, "no": 0, "Yes": 1, "No": 0}).fillna(0).astype(int)        
        trust_df = trust_df.append(xls_file[key].iloc[:, 0:9], ignore_index=True)


    ### PCN CALCULATIONS ###
    sheet_name = [sheet for sheet in sheets if sheet.startswith("PCN")]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine="openpyxl")
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex="Unnamed:")), axis=1, inplace=True)

        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        xls_file[key].rename(
            columns={
                list(xls_file[key])[0]: "For Month",
                list(xls_file[key])[1]: "ODS PCN Code",
                list(xls_file[key])[2]: "PCN Name",
                list(xls_file[key])[3]: "Partner Organisation connected to ShCR?",
                #extra column
                list(xls_file[key])[5]: "Partner Organisation plans to be connected by March 2023?",
            },
            inplace=True,
        )
        xls_file[key].insert(1, "ODS STP Code", STP_code, False)
        xls_file[key].insert(2, "STP Name", STP_name, False)
        xls_file[key].insert(3, "ICS Name (if applicable)", ICS_name, False)
        xls_file[key]["Partner Organisation connected to ShCR?"] = xls_file[key]["Partner Organisation connected to ShCR?"].map({"Connected": 1, "Not Connected": 0, "Please select": 0}).fillna(0) 
        xls_file[key]["Partner Organisation plans to be connected by March 2023?"] = xls_file[key]["Partner Organisation plans to be connected by March 2023?"].map({"yes": 1, "no": 0, "Yes": 1, "No": 0}).fillna(0)        
        pcn_df = pcn_df.append(xls_file[key].iloc[:, 0:9], ignore_index=True)
        
    ### LA CALCULATIONS ###
    sheet_name = [sheet for sheet in sheets if sheet.startswith("LA")]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine="openpyxl")
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex="Unnamed:")), axis=1, inplace=True)

        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        xls_file[key].rename(
            columns={
                list(xls_file[key])[0]: "For Month",
                list(xls_file[key])[1]: "ODS LA Code",
                list(xls_file[key])[2]: "LA Name",
                list(xls_file[key])[3]: "Partner Organisation connected to ShCR?",
                #extra column
                list(xls_file[key])[5]: "Partner Organisation plans to be connected by March 2023?",
            },
            inplace=True,
        )
        xls_file[key].insert(1, "ODS STP Code", STP_code, False)
        xls_file[key].insert(2, "STP Name", STP_name, False)
        xls_file[key].insert(3, "ICS Name (if applicable)", ICS_name, False)
        xls_file[key]["Partner Organisation connected to ShCR?"] = xls_file[key]["Partner Organisation connected to ShCR?"].map({"Connected": 1, "Not Connected": 0, "Please select": 0}).fillna(0)  
        xls_file[key]["Partner Organisation plans to be connected by March 2023?"] = xls_file[key]["Partner Organisation plans to be connected by March 2023?"].map({"yes": 1, "no": 0, "Yes": 1, "No": 0}).fillna(0)        
        la_df = la_df.append(xls_file[key].iloc[:, 0:9], ignore_index=True)

    ### Community Provider CALCULATIONS ###
    sheet_name = [sheet for sheet in sheets if sheet.startswith("Other Community")]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine="openpyxl")
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex="Unnamed:")), axis=1, inplace=True)

        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        xls_file[key].rename(
            columns={
                list(xls_file[key])[0]: "For Month",
                list(xls_file[key])[1]: "ODS Community Provider Code",
                list(xls_file[key])[2]: "Community Provider Name",
                list(xls_file[key])[3]: "Partner Organisation connected to ShCR?",
                #extra column
                list(xls_file[key])[5]: "Partner Organisation plans to be connected by March 2023?",
            },
            inplace=True,
        )
        xls_file[key].insert(1, "ODS STP Code", STP_code, False)
        xls_file[key].insert(2, "STP Name", STP_name, False)
        xls_file[key].insert(3, "ICS Name (if applicable)", ICS_name, False)
        xls_file[key]["Partner Organisation connected to ShCR?"] = xls_file[key]["Partner Organisation connected to ShCR?"].map({"Connected": 1, "Not Connected": 0, "Please select": 0}).fillna(0)   
        xls_file[key]["Partner Organisation plans to be connected by March 2023?"] = xls_file[key]["Partner Organisation plans to be connected by March 2023?"].map({"yes": 1, "no": 0, "Yes": 1, "No": 0}).fillna(0)        
        community_df = community_df.append(xls_file[key].iloc[:, 0:9], ignore_index=True) 

    ### Other Partner CALCULATIONS ###
    sheet_name = [sheet for sheet in sheets if sheet.startswith("Other partners")]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine="openpyxl")
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex="Unnamed:")), axis=1, inplace=True)

        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        xls_file[key].rename(
            columns={
                list(xls_file[key])[0]: "For Month",
                list(xls_file[key])[1]: "ODS Other Partner Code",
                list(xls_file[key])[2]: "Other Partner Name",
                list(xls_file[key])[3]: "Partner Organisation connected to ShCR?",
                #extra column
                list(xls_file[key])[5]: "Partner Organisation plans to be connected by March 2023?",
            },
            inplace=True,
        )
        xls_file[key].insert(1, "ODS STP Code", STP_code, False)
        xls_file[key].insert(2, "STP Name", STP_name, False)
        xls_file[key].insert(3, "ICS Name (if applicable)", ICS_name, False)
        xls_file[key]["Partner Organisation connected to ShCR?"] = xls_file[key]["Partner Organisation connected to ShCR?"].map({"Connected": 1, "Not Connected": 0, "Please select": 0}).fillna(0)
        xls_file[key]["Partner Organisation plans to be connected by March 2023?"] = xls_file[key]["Partner Organisation plans to be connected by March 2023?"].map({"yes": 1, "no": 0, "Yes": 1, "No": 0}).fillna(0)        
        other_df = other_df.append(xls_file[key].iloc[:, 0:9], ignore_index=True)            
        

    
# #Remove any non-required columns from final output
stp_df= stp_df[['For Month', 'ODS STP Code', 'STP Name', 'ICS Name (if applicable)', 'ShCR Programme Name', 'Name of ShCR System', 'Access to Advanced (EoL) Care Plans', 'Number of users with access to the ShCR', 'Number of ShCR views in the past month', 'Number of unique user ShCR views in the past month', 'Completed by (email)', 'Date completed']]

trust_df = trust_df[['For Month', 'ODS STP Code', 'STP Name', 'ICS Name (if applicable)', 'ODS Trust Code', 'Trust Name', 'Partner Organisation connected to ShCR?', 'Partner Organisation plans to be connected by March 2023?']]

pcn_df = pcn_df[['For Month', 'ODS STP Code', 'STP Name', 'ICS Name (if applicable)', 'ODS PCN Code', 'PCN Name', 'Partner Organisation connected to ShCR?', 'Partner Organisation plans to be connected by March 2023?']]

la_df = la_df[['For Month', 'ODS STP Code', 'STP Name', 'ICS Name (if applicable)', 'ODS LA Code', 'LA Name', 'Partner Organisation connected to ShCR?', 'Partner Organisation plans to be connected by March 2023?']]

community_df = community_df[['For Month', 'ODS STP Code', 'STP Name', 'ICS Name (if applicable)', 'ODS Community Provider Code', 'Community Provider Name', 'Partner Organisation connected to ShCR?', 'Partner Organisation plans to be connected by March 2023?']]

other_df = other_df[['For Month', 'ODS STP Code', 'STP Name', 'ICS Name (if applicable)', 'ODS Other Partner Code', 'Other Partner Name', 'Partner Organisation connected to ShCR?', 'Partner Organisation plans to be connected by March 2023?']]


# COMMAND ----------

stp_df

# COMMAND ----------

# #cast all date fields to datetime format and set to 1st of the month
# dt =lambda dt: dt.replace(day=1)

# pcn_df['For Month'] = pd.to_datetime(pcn_df['For Month']).apply(dt)
# stp_df['For Month'] = pd.to_datetime(stp_df['For Month']).apply(dt)
# trust_df['For Month'] = pd.to_datetime(trust_df['For Month']).apply(dt)

# COMMAND ----------

#Force data from folder name
# folder_date = pd.to_datetime(latestFolder) - pd.DateOffset(months=1)

# stp_df['For Month'] = folder_date
# stp_df['Date completed'] = pd.to_datetime(stp_df['Date completed'],errors='coerce')

# pcn_df['For Month'] = folder_date
# trust_df['For Month'] = folder_date

# COMMAND ----------

#CHECK FOR DUPLICATES
import collections

stp_dupes = [item for item, count in collections.Counter(stp_df['ODS STP Code']).items() if count > 1]
stp_dupes = stp_df[stp_df['ODS STP Code'].isin(stp_dupes)].sort_values(by='ODS STP Code')
stp_dupes = stp_dupes.iloc[:,[1,2,3,4,5,9]]

trust_dupes = [item for item, count in collections.Counter(trust_df['ODS Trust Code']).items() if count > 1]
trust_dupes = trust_df[trust_df['ODS Trust Code'].isin(trust_dupes)].sort_values(by='ODS Trust Code')
trust_dupes = trust_dupes.iloc[:,[1,2,4,5]]

pcn_dupes = [item for item, count in collections.Counter(pcn_df['ODS PCN Code']).items() if count > 1]
pcn_dupes = pcn_df[pcn_df['ODS PCN Code'].isin(pcn_dupes)].sort_values(by='ODS PCN Code')
pcn_dupes = pcn_dupes.iloc[:,[1,2,4,5]]

la_dupes = [item for item, count in collections.Counter(la_df['ODS LA Code']).items() if count > 1]
la_dupes = la_df[la_df['ODS LA Code'].isin(la_dupes)].sort_values(by='ODS LA Code')
la_dupes = la_dupes.iloc[:,[1,2,4,5]]

community_dupes = [item for item, count in collections.Counter(community_df['ODS Community Provider Code']).items() if count > 1]
community_dupes = community_df[community_df['ODS Community Provider Code'].isin(community_dupes)].sort_values(by='ODS Community Provider Code')
community_dupes = community_dupes.iloc[:,[1,2,4,5]]

other_dupes = [item for item, count in collections.Counter(other_df['ODS Other Partner Code']).items() if count > 1]
other_dupes = other_df[other_df['ODS Other Partner Code'].isin(community_dupes)].sort_values(by='ODS Other Partner Code')
other_dupes = other_dupes.iloc[:,[1,2,4,5]]

# COMMAND ----------

##Calculate aggregate numbers for Trusts
#------------------------------------
trust_count_df = trust_df.groupby('STP Name').agg(Total = ('Partner Organisation connected to ShCR?', 'size'), Connected = ('Partner Organisation connected to ShCR?', 'sum')).reset_index()
trust_count_df['Percent'] = trust_count_df['Connected']/trust_count_df['Total']

# ##Calculate aggregate numbers for PCNs
# #------------------------------------
pcn_count_df = pcn_df.groupby('STP Name').agg(Total = ('Partner Organisation connected to ShCR?', 'size'), Connected = ('Partner Organisation connected to ShCR?', 'sum')).reset_index()
pcn_count_df['Percent'] = pcn_count_df['Connected']/pcn_count_df['Total']

# ##Calculate aggregate numbers for LAs
# #------------------------------------
la_count_df = la_df.groupby('STP Name').agg(Total = ('Partner Organisation connected to ShCR?', 'size'), Connected = ('Partner Organisation connected to ShCR?', 'sum')).reset_index()
la_count_df['Percent'] = la_count_df['Connected']/la_count_df['Total']

# ##Calculate aggregate numbers for Community Providers
# #------------------------------------
community_count_df = community_df.groupby('STP Name').agg(Total = ('Partner Organisation connected to ShCR?', 'size'), Connected = ('Partner Organisation connected to ShCR?', 'sum')).reset_index()
community_count_df['Percent'] = community_count_df['Connected']/community_count_df['Total']

# ##Calculate aggregate numbers for Other
# #------------------------------------
other_count_df = other_df.groupby('STP Name').agg(Total = ('Partner Organisation connected to ShCR?', 'size'), Connected = ('Partner Organisation connected to ShCR?', 'sum')).reset_index()
other_count_df['Percent'] = other_count_df['Connected']/other_count_df['Total']

# COMMAND ----------

#SNAPSHOT SUMMARY
#Write pages to Excel file in iobytes
#--------------------------------------------------

files = [stp_df, stp_dupes, trust_df, trust_count_df, trust_dupes, pcn_df, pcn_count_df, pcn_dupes, la_df, la_count_df, la_dupes, community_df, community_count_df, community_dupes, other_df, other_count_df, other_dupes]

sheets = ['STP', 'STP dupes', 'Trust', 'Trust Count', 'Trust dupes', 'PCN', 'PCN Count', 'PCN dupes', 'LA', 'LA Count', 'LA dupes', 'Community', 'Community Count', 'Community dupes', 'Other', 'Other Count', 'Other dupes']

excel_sheet = io.BytesIO()

writer = pd.ExcelWriter(excel_sheet, engine='openpyxl')
for count, file in enumerate(files):
    file.to_excel(writer, sheet_name=sheets[count], index=False)
writer.save()

# COMMAND ----------

#SNAPSHOT SUMMARY
#Send Excel snapshot File to test Output in datalake
#--------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = excel_sheet
datalake_upload(file_contents, CONNECTION_STRING, file_system, "proc/projects/nhsx_slt_analytics/shcr/excel_summary/"+current_date_path, "shared_care_summary_output.xlsx")

# COMMAND ----------

#Pull historical files
#------------------------------------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, historical_source_path+latestFolder)
for file in file_name_list:
  if 'stp' in file:
    stp_df_historic = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, file)
    stp_df_historic = pd.read_parquet(io.BytesIO(stp_df_historic), engine="pyarrow")
    stp_df_historic['For Month'] = pd.to_datetime(stp_df_historic['For Month'])
    stp_df_historic['Date completed'] = pd.to_datetime(stp_df_historic['Date completed'],errors='coerce')
  if 'pcn' in file:
    pcn_df_historic = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, file)
    pcn_df_historic = pd.read_parquet(io.BytesIO(pcn_df_historic), engine="pyarrow")
    pcn_df_historic['For Month'] = pd.to_datetime(pcn_df_historic['For Month'])
  if 'trust' in file:
    trust_df_historic = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, file)
    trust_df_historic = pd.read_parquet(io.BytesIO(trust_df_historic), engine="pyarrow")
    trust_df_historic['For Month'] = pd.to_datetime(trust_df_historic['For Month'])
    

# COMMAND ----------

# Append new data to historical data
#-----------------------------------------------------------------------

#STP
#--------------------------------------------------------------
dates_in_historic = stp_df_historic["For Month"].unique().tolist()
dates_in_new = stp_df["For Month"].unique().tolist()[0]

if dates_in_new in dates_in_historic:
  print('STP Data already exists in historical STP data')
else:
  stp_df_historic = stp_df_historic.append(stp_df)
  stp_df_historic = stp_df_historic.sort_values(by=['For Month'])
  stp_df_historic = stp_df_historic.reset_index(drop=True)
  stp_df_historic = stp_df_historic.astype(str)
  
# PCN
#--------------------------------------------------------------
dates_in_historic = pcn_df_historic["For Month"].unique().tolist()
dates_in_new = pcn_df["For Month"].unique().tolist()[0]

if dates_in_new in dates_in_historic:
  print('PCN Data already exists in historical STP data')
else:
  pcn_df_historic = pcn_df_historic.append(pcn_df)
  pcn_df_historic = pcn_df_historic.sort_values(by=['For Month'])
  pcn_df_historic = pcn_df_historic.reset_index(drop=True)
  pcn_df_historic = pcn_df_historic.astype(str)
  
#TRUST
#--------------------------------------------------------------
dates_in_historic = trust_df_historic["For Month"].unique().tolist()
dates_in_new = trust_df["For Month"].unique().tolist()[0]

if dates_in_new in dates_in_historic:
  print('Trust Data already exists in historical STP data')
else:
  trust_df_historic = trust_df_historic.append(trust_df)
  trust_df_historic = trust_df_historic.sort_values(by=['For Month'])
  trust_df_historic = trust_df_historic.reset_index(drop=True)
  trust_df_historic = trust_df_historic.astype(str)

# COMMAND ----------

# Upload processed data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()

# PCN
#-----------------
pcn_df_historic.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, "shcr_partners_pcn_data_month_count.parquet")

# STP
#-----------------
file_contents = io.BytesIO()
stp_df_historic.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, "shcr_partners_stp_data_month_count.parquet")

# NHS Trust
#-----------------
file_contents = io.BytesIO()
trust_df_historic.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, "shcr_partners_trust_data_month_count.parquet")
