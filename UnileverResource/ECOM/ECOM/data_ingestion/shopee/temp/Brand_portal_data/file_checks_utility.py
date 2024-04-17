# Databricks notebook source
#Check existed file: # Define funtions
def file_exists(dataset_name, path):
  try:
    dbutils.fs.ls(path)
    print(dataset_name, ": file is downloaded sucessfully")
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise