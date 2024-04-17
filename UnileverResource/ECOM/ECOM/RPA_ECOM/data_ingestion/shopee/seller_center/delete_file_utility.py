# Databricks notebook source
def delete_file():
  print('entered in delete method')
  if len(glob.glob("/tmp/Shopee_Seller_Center/*")) > 0:
    for file in glob.glob("/tmp/Shopee_Seller_Center/*"):
      if len(file)!=0:
        os.remove(file)
        print("Deleted :" + str(file))
  else:
    print('No file found to delete')

# COMMAND ----------



# COMMAND ----------

