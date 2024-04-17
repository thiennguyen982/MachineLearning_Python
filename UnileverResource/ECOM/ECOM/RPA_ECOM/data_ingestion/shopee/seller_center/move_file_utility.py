# Databricks notebook source
def get_adls_path(adls_base_path,shop_name, data_set_name, date_range, year, month_name):
   
    if shop_name == "Michiru official store":
      shop_name_folder = 'unilever_beauty_premium'      
    elif shop_name == "Unilever Việt Nam _ Health & Beauty":
      shop_name_folder = 'unilevervn_beauty'      
    else:
      shop_name_folder = 'unilever_vietnam'
        
        
    if data_set_name == "Product Performance":
      data_set_name = 'product_performance'
      data_set_folder = 'product/product_performance'   
    elif data_set_name == "Traffic Overview":
      data_set_name = 'traffic_overview'
      data_set_folder = 'traffic/traffic_overview'
    elif data_set_name == "Flash Sale":
      data_set_name = 'flash_sale'
      data_set_folder = 'marketing/flash_sale'     
    else:
      data_set_name = 'livestream'
      data_set_folder = 'marketing/livestream'
      
     #get date_range folder 
    if date_range == "Yesterday":
      date_range_folder = 'yesterday'      
    elif date_range == "Past 7 Days":
      date_range_folder = 'past_7_days'      
    elif date_range == "Past 30 Days":
      date_range_folder = 'past_30_days'      
    else:
      date_range_folder = str(year)+'/'+month_name
    
    if date_range == 'By Monthly':
      file_name_form = shop_name_folder+'_'+data_set_name+'_'
    else:
      file_name_form = shop_name_folder+'_'+data_set_name+'_'+date_range_folder+'_'
      
    temp_adls_path = adls_base_path + shop_name_folder +'/'+ data_set_folder +'/'+ date_range_folder +'/'
    
    return file_name_form, temp_adls_path

# COMMAND ----------

def delete_adls_file(full_adls_path):
  #print('entered in delete method')
  temp_path = '/dbfs'+full_adls_path[5:]
  try:
    for file_name in listdir(temp_path):
      os.remove(temp_path + file_name)
      print("Deleted file from : ",temp_path + file_name)
      
  except:
    print('No file found to delete in ',full_adls_path)

# COMMAND ----------

# path = 'dbfs:/mnt/adls/landing/shopee/seller_center/unilever_beauty_premium/marketing/flash_sale/yesterday/'
# print('/dbfs'+path[5:])

# COMMAND ----------

# # import glob
# # for file in glob.glob("dbfs:/mnt/adls/landing/shopee/seller_center/unilever_beauty_premium/marketing/flash_sale/yesterday/*"):
# #   print(file)      

# import os 
# from os import listdir
# my_path = '/dbfs/mnt/adls/landing/shopee/seller_center/unilever_beauty_premium/marketing/flash_sale/yesterday/'
# for file_name in listdir(my_path):
#     print(file_name)
#     #os.remove(my_path + file_name)

# COMMAND ----------

# def move_files_to_adls(shop_name, data_set_name, date_range, year, month_name):
  
#   source = 'file:/tmp/Shopee_Seller_Center/'
#   adls_basepath = 'dbfs:/mnt/adls/landing/shopee/seller_center/'
#   file_name_form, adls_path = get_adls_path(adls_basepath, shop_name, data_set_name, date_range, year, month_name)
  
#   print("file_name_form: ",file_name_form)
#   print("adls_path: ",adls_path)
  
#   try:
#     adb_base_path = '/tmp/Shopee_Seller_Center/'
#     file_full_path = max([os.path.join(adb_base_path, f) for f in os.listdir(adb_base_path)], key=os.path.getctime)
#     print('latest file: ',file_full_path)
#     date_period = file_full_path[-22:]
#     print('date_period: ', date_period)
#     os.rename(file_full_path , adb_base_path+file_name_form+date_period)
#     renamed_file_name = max([os.path.join(adb_base_path, f) for f in os.listdir(adb_base_path)], key=os.path.getmtime)
#     final_file_name = os.path.basename(renamed_file_name)
#     print(final_file_name)
#   except:
#     print('No recent file created in ADB')
      
#   try:
    
#     if len(final_file_name) != 0:
#       dbutils.fs.cp(source + final_file_name, adls_path+final_file_name)
#       print('moved file is: ', source + final_file_name)
#     else:
#       print('file is not downloaded')
       
#   except:
#     print('No files are moved to ADLS')

# COMMAND ----------

def move_files_to_adls(shop_name, data_set_name, date_range, year, month_name):
  
  source = 'file:/tmp/Shopee_Seller_Center/'
  adls_basepath = 'dbfs:/mnt/adls/landing/shopee/seller_center/'
  file_name_form, adls_path = get_adls_path(adls_basepath, shop_name, data_set_name, date_range, year, month_name)
  
  #print("file_name_form: ",file_name_form)
  #print("adls_path: ",adls_path)
  
  try:
    adb_base_path = '/tmp/Shopee_Seller_Center/'
    file_full_path = max([os.path.join(adb_base_path, f) for f in os.listdir(adb_base_path)], key=os.path.getctime)
    #print('latest file: ',file_full_path)
    date_period = file_full_path[-22:]
    #print('date_period: ', date_period)
    os.rename(file_full_path , adb_base_path+file_name_form+date_period)
    renamed_file_name = max([os.path.join(adb_base_path, f) for f in os.listdir(adb_base_path)], key=os.path.getmtime)
    final_file_name = os.path.basename(renamed_file_name)
    #print(final_file_name)
  except:
    print('No recent file is created in ADB')
      
  try:
    
    if len(final_file_name) != 0:
      if date_range == 'Yesterday' or date_range == 'Past 7 Days' or date_range == 'Past 30 Days':
        delete_adls_file(adls_path)
        daily_path = adls_path[:-10] + 'daily/'
        #print(daily_path)
        if date_range == 'Yesterday':
          dbutils.fs.cp(source + final_file_name, daily_path+final_file_name)
          print('moved file to: ', daily_path + final_file_name)
        
        dbutils.fs.cp(source + final_file_name, adls_path+final_file_name)
        print('moved file to: ', adls_path + final_file_name)
      else:
        dbutils.fs.cp(source + final_file_name, adls_path+final_file_name)
        print('moved file to: ', adls_path + final_file_name)
    else:
      print('file is not downloaded')
       
  except:
    print('No files are moved to ADLS')

# COMMAND ----------

