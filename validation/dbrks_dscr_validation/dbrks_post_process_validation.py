# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_post_process_validation.py
DESCRIPTION:
                Databricks notebook for digital social care postprocessing validation
USAGE:
                ...
CONTRIBUTORS:   Abdu Nuhu
CONTACT:        nhsx.data@england.nhs.uk
CREATED:        30 Jan. 2023
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# ------------------------------------------------------------------------------------
%pip install pandas pathlib azure-storage-file-datalake numpy pyarrow==5.0.* great_expectations

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
import great_expectations as ge
# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Shared/databricks/au-azure-databricks-cicd/functions/dbrks_helper_functions

# COMMAND ----------

# Load parameters and JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "config/pipelines/nhsx-au-analytics"
file_name_config = "config_dscr_dbrks.json"
log_table = "dbo.post_load_log"

file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope="AzureDataLake", key="DATALAKE_CONTAINER_NAME")
month_count_tbl = config_JSON["pipeline"]["staging"][0]["sink_table"]
collated_count_tbl = config_JSON["pipeline"]["staging"][1]["sink_table"]

print("----------------- Tables to check ---------------")
print(month_count_tbl)
print(collated_count_tbl)
print("-----------------------------------------------------")


# COMMAND ----------


month_df = read_sql_server_table(month_count_tbl)
today_month_count = month_df.count()
pd_month_df = month_df.toPandas()

today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')


# COMMAND ----------

# validate data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
val_df = pd_month_df.mask(pd_month_df == " ") # convert all blanks to NaN for validtion
validation_df = ge.from_pandas(val_df) # Create great expectations dataframe from pandas datafarme


# COMMAND ----------

agg_type = "Count of dscr_all_variables_month_count table"
month_count_agg = get_post_load_agg(log_table, month_count_tbl, agg_type)
today_previous_validation(month_count_agg, month_count_tbl, 20, validation_df, agg_type)


# COMMAND ----------

in_row = {"load_date":[date], "tbl_name":[month_count_tbl], "aggregation":agg_type, "aggregate_value":[today_month_count]}
print("----------- Record to write in table --------------")
print(in_row)
print("----------------------------------------------------")
df = pd.DataFrame(in_row)
write_to_sql(df, log_table, "append")
