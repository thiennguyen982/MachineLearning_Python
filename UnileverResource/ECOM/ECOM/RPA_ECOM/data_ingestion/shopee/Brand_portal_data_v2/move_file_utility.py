# Databricks notebook source
def get_adls_path(dataset_name, from_date, to_date):
  
  search_str = ''
  path_search_str = []
  
  if dataset_name == 'market solutions product':
    search_str = 'Traffic_Report---Product_Performance'
    folder_name = 'marketing_solutions'
  elif dataset_name == 'market solutions campaign':
    search_str = 'Campaign_Performance'
    folder_name = 'marketing_solutions'
  elif dataset_name == 'sales shop':
    search_str = 'Shop_Dashboard'
    folder_name = 'sales'
  elif dataset_name == 'sales product':
    search_str = 'Product_Analysis---Product_Performance'
    folder_name = 'sales'
  elif dataset_name == 'flash sale':
    search_str = 'Flash_Sales_Performance'
    folder_name = 'sales'
  else:
    print('file not found')
    
  print(search_str)
    
  if len(search_str) > 1:
    
    if (from_date.year == to_date.year) and (from_date.month == to_date.month):
      days = monthrange(to_date.year, to_date.month)
      #print(days[1])
      number_of_days_for_given_month = days[1]
      #print("number_of_days_for_given_month:", number_of_days_for_given_month)
      number_of_days_between_given_dates = to_date.day-from_date.day+1
      #print("number_of_days_between_given_dates:", number_of_days_between_given_dates)
      
      if number_of_days_for_given_month == number_of_days_between_given_dates:
        str_month = month_check(to_date)
        
        source = 'file:/tmp/Shopee/'
        adls_destination = 'dbfs:/mnt/adls/landing/shopee/brand_portal/' + folder_name + '/' + str(to_date.year) + '/' + str_month + '/'
        #print(adls_destination)
        path_search_str.append(adls_destination)
        path_search_str.append(search_str)
        return path_search_str
        
      else:
        
        source = 'file:/tmp/Shopee/'
        adls_destination = 'dbfs:/mnt/adls/landing/shopee/brand_portal/' + folder_name + '/' + 'temp/'
        #print(adls_destination)
        
        path_search_str.append(adls_destination)
        path_search_str.append(search_str)
        return path_search_str 
      
    else:
      source = 'file:/tmp/Shopee/'
      adls_destination = 'dbfs:/mnt/adls/landing/shopee/brand_portal/' + folder_name + '/' + 'temp/'
      #print(adls_destination)
      
      path_search_str.append(adls_destination)
      path_search_str.append(search_str)
      return path_search_str
        
  else:
    print('No files are moved to ADLS')
    path_search_str
    
#get_adls_path(dataset_name)

# COMMAND ----------

def move_files_to_adls(dataset_name, from_date, to_date):
  
  source = 'file:/tmp/Shopee/'
  path_and_search_str = get_adls_path(dataset_name, from_date, to_date)
  
  try:
    if len(path_and_search_str) == 2:
      adls_destination = path_and_search_str[0]
      search_str = path_and_search_str[1]
      
      files_list = os.listdir('/tmp/Shopee/')
      
      for file in files_list:
        if search_str in file:
          dbutils.fs.cp(source+file, adls_destination+file)
          print('moved file is: ', file)
    
    else:
      print('Did not find a ADLS path')    
    
  except:
    print('No files are moved to ADLS')
  
     
#move_files_to_adls(dataset_name, from_date, to_date)

# COMMAND ----------

