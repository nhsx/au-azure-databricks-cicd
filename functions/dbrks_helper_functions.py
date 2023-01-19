# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_helper_functions.py
DESCRIPTION:
                Helper functons needed for the databricks processing step of ETL pipelines
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Kabir Khan, Faaiz Shanawas
CONTACT:        data@nhsx.nhs.uk
CREATED:        11 Jan 2023
VERSION:        0.0.2
"""

# COMMAND ----------

# Helper functions
# -------------------------------------------------------------------------
def datalake_download(CONNECTION_STRING, file_system, source_path, source_file):
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    directory_client = file_system_client.get_directory_client(source_path)
    file_client = directory_client.get_file_client(source_file)
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    return downloaded_bytes
  
def datalake_upload(file, CONNECTION_STRING, file_system, sink_path, sink_file):
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    directory_client = file_system_client.get_directory_client(sink_path)
    file_client = directory_client.create_file(sink_file)
    file_length = file_contents.tell()
    file_client.upload_data(file_contents.getvalue(), length=file_length, overwrite=True)
    return '200 OK'
  
def datalake_latestFolder(CONNECTION_STRING, file_system, source_path):
  try:
      service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
      file_system_client = service_client.get_file_system_client(file_system=file_system)
      pathlist = list(file_system_client.get_paths(source_path))
      folders = []
      # remove file_path and source_file from list
      for path in pathlist:
        folders.append(path.name.replace(source_path.strip("/"), "").lstrip("/").rsplit("/", 1)[0])
        folders.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"), reverse=True)
      latestFolder = folders[0]+"/"
      return latestFolder
  except Exception as e:
      print(e)

def write_to_sql(df_processed, table_name, write_mode = str):
  sparkDF=spark.createDataFrame(df_processed)
  server_name = dbutils.secrets.get(scope="sqldatabase", key="SERVER_NAME")
  database_name = dbutils.secrets.get(scope="sqldatabase", key="DATABASE_NAME")
  url = server_name + ";" + "databaseName=" + database_name + ";"
  username = dbutils.secrets.get(scope="sqldatabase", key="USER_NAME")
  password = dbutils.secrets.get(scope="sqldatabase", key="PASSWORD")
  try:
    sparkDF.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode(write_mode) \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .save()
    return print("Connector write succeed")
  except ValueError as error:
    return print("Connector write failed", error)

def write_spark_df_to_sql(sparkDF, table_name, write_mode = str):
  server_name = dbutils.secrets.get(scope="sqldatabase", key="SERVER_NAME")
  database_name = dbutils.secrets.get(scope="sqldatabase", key="DATABASE_NAME")
  url = server_name + ";" + "databaseName=" + database_name + ";"
  username = dbutils.secrets.get(scope="sqldatabase", key="USER_NAME")
  password = dbutils.secrets.get(scope="sqldatabase", key="PASSWORD")
  try:
    sparkDF.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode(write_mode) \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .save()
    return print("Connector write succeed")
  except ValueError as error:
    return print("Connector write failed", error)

# Read SQL Table ------------------
def read_sql_server_table(table_name):
  server_name = dbutils.secrets.get(scope="sqldatabase", key="SERVER_NAME")
  database_name = dbutils.secrets.get(scope="sqldatabase", key="DATABASE_NAME")
  url = server_name + ";" + "databaseName=" + database_name + ";"
  username = dbutils.secrets.get(scope="sqldatabase", key="USER_NAME")
  password = dbutils.secrets.get(scope="sqldatabase", key="PASSWORD")
  try:
    sparkDF = spark.read \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .load()
    print("Connector write succeed")
    return sparkDF
  except ValueError as error:
    return print("Connector write failed", error)

# COMMAND ----------

# Ingestion and analytical functions
# -------------------------------------------------------------------------
def ons_geoportal_file_download(search_url, url_start, string_filter):
  url_2 = '/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
  page = requests.get(search_url)
  response = urlreq.urlopen(search_url)
  soup = BeautifulSoup(response.read(), "lxml")
  data_url = soup.find_all('a', href=re.compile(string_filter))[-1].get('href')
  full_url = url_start + data_url + url_2
  with urlopen(full_url)  as response:
      json_file = json.load(response)
  return json_file

def datalake_listContents(CONNECTION_STRING, file_system, source_path):
  try:
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    folder_path = file_system_client.get_paths(path=source_path)
    file_list = []
    for path in folder_path:
      file_list.append(path.name.replace(source_path.strip("/"), "").lstrip("/").rsplit("/", 1)[0])
    return file_list
  except Exception as e:
    print(e)
    
#Renaming columns for the DSPT GP dataframe
def rename_dspt_gp_cols(df):
  column_list = []
  column_name = ''
  for cols in df.columns:
    column_list.append(cols)
  for col in column_list:
    if "PRIMARY" in col.upper(): #.upper() is used in case some headings are lower case or upper case
      df.rename(columns={col:"Primary Sector"}, inplace = True)
    elif "CODE" in col.upper():
      df.rename(columns={col:"Code"}, inplace = True)
    elif "ORGANISATION" in col.upper():
      df.rename(columns={col:"Organisation_Name"}, inplace = True)
    elif "STATUS" in col.upper():
      df.rename(columns={col:"Status_Raw"}, inplace = True)
  return df #returns the renamed dataframe

def process_dspt_dataframe(df, filename, year):
  df = rename_dspt_gp_cols(df) #call the rename_cols function on the dataframe to syncronise all the column names
  df_gp = df[df["Primary Sector"] == "GP"] #filter the primary sector column for GP's only 
  num_rows = df_gp.shape[0] #retrieve the number of rows after filtering to be create the right size columns(lists) for the dspt_edition and snapshot_date
  dspt_edition = get_dspt_edition(year, num_rows) #call and save the columns(list) for dspt_edition and snapshot_date
  snapshot_date = get_snapshot_date(year, filename, num_rows)
  df_gp = df_gp.assign(DSPT_Edition = dspt_edition) #assign these columns to the dataframe
  df_gp = df_gp.assign(Snapshot_Date = snapshot_date)
  df_gp = df_gp[['Code', 'Organisation_Name', 'DSPT_Edition', 'Snapshot_Date', 'Status_Raw']] #drop all the columns we do not need for the final curation and keep all relevant ones as specified below
  df_gp = df_gp.reset_index(drop = True) #reset the indexes as these will all be different after filtering
  return df_gp

#fetching data from the filename - get dspt edition and snapshot date for appending to new dataframe
def get_year_dspt_gp(upload_date):
  year = int(upload_date[0:4])
  return year

def get_dspt_edition(year, num_rows):
  dspt_edition = str(year - 1) + "/" + str(year) #specifies format as previous_year/current_year
  dspt_edition_list = [dspt_edition] * num_rows #creates the column of size num_rows to be added to dataframe
  return dspt_edition_list 

def get_snapshot_date(year, filename, num_rows):
  filename_sep = filename.split()
  index = 0
  for word in filename_sep:
    if str(year) in word:
      index = filename_sep.index(word)
  date = filename_sep[index]
  date = date.replace("_", "/")  
  snapshot_date_list = [date] * num_rows
  return snapshot_date_list

#this function is ued in set_flag_conditions to check if the current DSPT status contains years in their labels by checking for digits in the status
def contains_digits(input_string):
  flag = False
  for character in input_string:
    if character.isdigit() == True:
      flag = True
  return flag #returns a true value if the status has got digits and false otherwise

# COMMAND ----------

# Validation Helper Function
#-----------------------------------------
def test_result(great_expectation_result, test_info):
    test_outcome = 'DID NOT RUN SUCCESSFULLY'
    expectation_result = str(great_expectation_result)
    test_result = json.loads(expectation_result)
    result = test_result['success']
    if result == True:
      test_outcome = 'PASS'
    elif result == False:
      test_outcome = 'FAILED'
        
    print('#############################################################################')
    print(test_info + ' Result: ' + test_outcome)
    print('##############################-----END-----##################################')

# COMMAND ----------

# Function to get the latest row count from the log table dbo.pre_load_log
#----------------------------------------------------------------------
def get_latest_count(log_count_tbl, file_nane):
    spark_count_df = read_sql_server_table(log_count_tbl)
    count_df = spark_count_df.toPandas()
    count_df_2 = count_df[count_df["file_to_load"].str.contains(file_nane)]
    last_run_date = count_df_2["load_date"].max()
    count_df_3 = count_df_2[count_df_2["load_date"] == last_run_date]
    return count_df_3


# COMMAND ----------

# Function to get the latest aggregationfrom the log table dbo.pre_load_agg_log
#-------------------------------------------------------------------------------
def get_last_agg(agg_log_tbl, file_nane, agg_name, col_info):
    spark_count_agg_df = read_sql_server_table(agg_log_tbl) 
    agg_df = spark_count_agg_df.toPandas() 
    agg_df_2 = agg_df[(agg_df["file_name"].str.contains(file_nane)) & (agg_df["aggregation"] == agg_name) & (agg_df["comment"] == col_info)] 
    last_run_date = agg_df_2["load_date"].max()    
    agg_df_3 = agg_df_2[agg_df_2["load_date"] == last_run_date]
    return agg_df_3
  

# COMMAND ----------


