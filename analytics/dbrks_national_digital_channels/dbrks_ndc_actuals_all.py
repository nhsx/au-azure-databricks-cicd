# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_ndc_actuals_all.py
DESCRIPTION:
                Databricks notebook with processing code for all actuals
USAGE:
                ...
CONTRIBUTORS:   Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        16th Nov 2023
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
file_name_config = "config_national_digital_channels_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
#source_file = config_JSON['pipeline']['project']["source_file_forecasts"]
source_file_daily = 'national_digital_channels_historical_daily.parquet'
source_file_monthly = 'national_digital_channels_historical_monthly.parquet'
source_file_messages = 'national_digital_channels_historical_messages.parquet'

#sink_path = config_JSON['pipeline']['project']['databricks'][48]['sink_path']
sink_path = 'proc/projects/nhsx_slt_analytics/national_digital_channels/ndc_actuals_all/'

#sink_file = config_JSON['pipeline']['project']['databricks'][48]['sink_file']  
sink_file = 'ndc_actuals_all.csv'

#table_name = config_JSON['pipeline']["staging"][48]['sink_table']
table_name = 'ndc_actuals_all'

# COMMAND ----------

#Ingestion of data
# ---------------------------------------------------------------------------------------------------

#Daily#
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file_daily)
daily_data_df  = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
daily_data_df = daily_data_df.rename(columns={'Daily':'Date'})
daily_data_df = daily_data_df.resample('MS', on='Date').sum()

#Monthly#
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file_monthly)
monthly_data_df  = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
monthly_data_df = monthly_data_df.rename(columns={'Monthly':'Date'})

#Messages#
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file_messages)
messages_data_df  = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
messages_data_df = messages_data_df.rename(columns={'Daily':'Date'})
messages_data_df = messages_data_df.resample('MS', on='Date').sum()

#joining#
monthly_data_df = monthly_data_df.merge(daily_data_df, how='outer', on = 'Date')
monthly_data_df = monthly_data_df.merge(messages_data_df, how='outer', on = 'Date')
monthly_data_df = monthly_data_df.drop(monthly_data_df.columns[monthly_data_df.columns.str.contains('unnamed',case = False)],axis = 1)
monthly_data_df = monthly_data_df.drop(monthly_data_df.columns[monthly_data_df.columns.str.contains('deprecated',case = False)],axis = 1)

monthly_data_df.index.name = "Unique ID"
df_processed = monthly_data_df.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------
write_to_sql(df_processed, table_name, "overwrite")
