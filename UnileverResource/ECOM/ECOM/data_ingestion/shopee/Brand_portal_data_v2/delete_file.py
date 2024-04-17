# Databricks notebook source
# MAGIC %run ./move_file_utility

# COMMAND ----------

def delete_file(dataset_name, from_date, to_date):
  
  path_and_search_str = get_adls_path(dataset_name, from_date, to_date)  
  search_str= path_and_search_str[1]  
  
  for file in glob.glob("/tmp/Shopee/*"):
    if search_str in file:
      os.remove(file)
      print("Deleted :" + str(file))

# COMMAND ----------

