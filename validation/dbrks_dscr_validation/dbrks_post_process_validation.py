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
from pyspark.sql.functions import count

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

today = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
date = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Beging Test

# COMMAND ----------

# MAGIC %md
# MAGIC ### Month count

# COMMAND ----------

# -------------------------------------------------------------------------------------------------------------------------------------------------------
# read data from dscr_all_variables_month_count table and count rows. At this stage the data in the table has been refresh and contains additional new data
month_df = read_sql_server_table(month_count_tbl)
today_month_count = month_df.count()
pd_month_df = month_df.toPandas()


# COMMAND ----------

# validate data
# Greate expectations https://www.architecture-performance.fr/ap_blog/built-in-expectations-in-great-expectations/
# ----------------------------------
month_val_df = pd_month_df.mask(pd_month_df == " ") # convert all blanks to NaN for validtion
month_validation_df = ge.from_pandas(month_val_df) # Create great expectations dataframe from pandas datafarme, this holds data from today's files


# COMMAND ----------

# ------------------------------------------------------------------------------------------------------------------------------------
# Validating that post procesing row are within accepatble count range for dscr_all_variables_month_count 
agg_type = "Count of dscr_all_variables_month_count table"
month_count_agg = get_post_load_agg(log_table, month_count_tbl, agg_type) # get previous row count from log before logging today's count
today_previous_validation(month_count_agg, month_count_tbl, 20, month_validation_df, agg_type)


# COMMAND ----------

info = "Checking that ICB_ONS_Code column in all_variables_month_count does not have NULLs"
expect = month_validation_df.expect_column_values_to_not_be_null("ICB_ONS_Code") # Check that has no null or blank values
test_result(expect, info)
assert expect.success

# COMMAND ----------

# Checking that month_year coulumn is ncreasing
# -----------------------------------------------------------------
distinct_month_year = pd_month_df["month_year"].unique()
month_year_distinct_df = pd.DataFrame(data = distinct_month_year, columns = ["month_year"])
month_year_distinct_df["month_year"] = pd.to_datetime(month_year_distinct_df["month_year"])
month_year_distinct_df["month_year"] = month_year_distinct_df["month_year"].dt.strftime("%Y%m%d").astype(int)
month_year_distinct_df.sort_values(by="month_year", ascending=True, inplace=True)

distinct_ge_df = ge.from_pandas(month_year_distinct_df)
info = "Checking that month_year column is increasing"
expect = distinct_ge_df.expect_column_values_to_be_increasing(column="month_year")
test_result(expect, info)
assert expect.success

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collated

# COMMAND ----------

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# read data from dbrks_dscr_all_variables_collated_count table and count rows. At this stage the data in the table has been refresh and contains additional new data
collated_df = read_sql_server_table(collated_count_tbl)
today_collated_count = collated_df.count()
pd_collated_df = collated_df.toPandas()
location_id_count = pd_collated_df["Location_Id"].count()

print("######################### Today's Location_Id count is shown below ###############################")
print(location_id_count)

# COMMAND ----------

# ----------------------------------
# GE dataframe for dbrks_dscr_all_variables_collated_count table
collated_val_df = pd_collated_df.mask(pd_collated_df == " ") # convert all blanks to NaN for validtion
collated_validation_df = ge.from_pandas(collated_val_df) # Create great expectations dataframe from pandas datafarme, this holds data from today's files

# COMMAND ----------

# ------------------------------------------------------------------------------------------------------------------------------------
# Validating that post procesing row are within accepatble count range for dbrks_dscr_all_variables_collated_count
agg_type_dscr = "Count of dbrks_dscr_all_variables_collated_count table"
collated_count_agg = get_post_load_agg(log_table, collated_count_tbl, agg_type_dscr) # get previous row count from log before logging today's count
today_previous_validation(collated_count_agg, collated_count_tbl, 10, collated_validation_df, agg_type_dscr)


# COMMAND ----------

info = "Checking that ICB_ONS_Code column in all_variables_collated does not have NULLs"
expect = collated_validation_df.expect_column_values_to_not_be_null("ICB_ONS_Code") # Check that has no null or blank values
test_result(expect, info)
assert expect.success

# COMMAND ----------

info = "Checking that Location_Id column is unique"
expect = collated_validation_df.expect_column_values_to_be_unique(column="Location_Id")
test_result(expect, info)
assert expect.success

# COMMAND ----------

info = "Checking that Location_Id column does not have NULLs"
expect = collated_validation_df.expect_column_values_to_not_be_null("Location_Id") # Check that has no null or blank values
test_result(expect, info)
assert expect.success

# COMMAND ----------

# ------------------------------------------------------------------------------------------------------------------------------------
# Validating that post procesing row count is within acceptable range for Location_Id column
collated_location_agg = "Count of distinct Location_Id column"
collated_location_id_prev_count = get_post_load_agg(log_table, collated_count_tbl, collated_location_agg)
post_load_unique_column_validation(collated_location_id_prev_count, collated_count_tbl, 5, collated_validation_df, collated_location_agg, "Location_Id")

# COMMAND ----------

# ------------------------------------------------------------------------------------------------------------------------------------*****
# Validating that post procesing row count for Yes in Use a Digital Social Care Record system? column is about same or has increased
collated_yes_agg = "Count of Yes for Use a Digital Social Care Record system column is about same or has increased"
yes_df = pd.DataFrame(pd_collated_df["Use a Digital Social Care Record system?"].str.upper())
yes_only_df = yes_df[yes_df["Use a Digital Social Care Record system?"] == "YES"]
today_yes_collated_count = len(yes_only_df)

print("############### Today's count of YES is shown below #######################")
print(today_yes_collated_count)
print("############################################################################")

collated_yes_prev_count = get_post_load_agg(log_table, collated_count_tbl, collated_yes_agg)
yes_ge_df = ge.from_pandas(yes_only_df)
today_previous_validation(collated_yes_prev_count, collated_count_tbl, 0.1, yes_ge_df, collated_yes_agg)
  


# COMMAND ----------

# MAGIC %md
# MAGIC ## End Test

# COMMAND ----------

# ------------------------------------------------------------------------------------------------------------------------------------
# Log today's row count for dscr_all_variables_month_count table
in_row = {"load_date":[date], "tbl_name":[month_count_tbl], "aggregation":agg_type, "aggregate_value":[today_month_count]}
print("----------- Record to write in table --------------")
print(in_row)
print("----------------------------------------------------")
df = pd.DataFrame(in_row)
write_to_sql(df, log_table, "append")

# COMMAND ----------

# ------------------------------------------------------------------------------------------------------------------------------------
# Log today's row count for dscr_all_variables_collated_count table
in_row = {"load_date":[date], "tbl_name":[collated_count_tbl], "aggregation":agg_type_dscr, "aggregate_value":[today_collated_count]}
print("----------- Record to write in table --------------")
print(in_row)
print("----------------------------------------------------")
df = pd.DataFrame(in_row)
write_to_sql(df, log_table, "append")

# COMMAND ----------

# ------------------------------------------------------------------------------------------------------------------------------------
# Log today's unique row count for Location_Id Column
in_row = {"load_date":[date], "tbl_name":[collated_count_tbl], "aggregation":collated_location_agg, "aggregate_value":[location_id_count]}
print("----------- Record to write in table --------------")
print(in_row)
print("----------------------------------------------------")
df = pd.DataFrame(in_row)
write_to_sql(df, log_table, "append")

# COMMAND ----------

# Log today's row count for Use a Digital Social Care Record system? Column with Yes ***
in_row = {"load_date":[date], "tbl_name":[collated_count_tbl], "aggregation":collated_yes_agg, "aggregate_value":[today_yes_collated_count]}
print("----------- Record to write in table --------------")
print(in_row)
print("----------------------------------------------------")
df = pd.DataFrame(in_row)
write_to_sql(df, log_table, "append")
