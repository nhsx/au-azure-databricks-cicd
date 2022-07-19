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
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        01 Sept 2021
VERSION:        0.0.1
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

# COMMAND ----------

# Functions for orchestrator 
# -------------------------------------------------------------------------
class NotebookData:
    def __init__(self, path, timeout):
        self.path = path
        self.timeout = timeout
        
def submitNotebook(notebook):
    print("Running notebook %s" % notebook.path)
    try:
        return dbutils.notebook.run(notebook.path, notebook.timeout)
    except Exception as e:
        print(e)
        raise Exception()
            
def parallelNotebooks(notebooks, numInParallel):
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [ec.submit(submitNotebook, notebook) for notebook in notebook_list]

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
