# Databricks notebook source
def path_exists(file_path):
    try:
        dbutils.fs.ls(file_path)
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
              return False
        else:
              raise e
    return True