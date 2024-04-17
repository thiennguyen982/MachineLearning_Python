# Databricks notebook source
# def get_sales_shop_data(browser,from_date, to_date):
#   url = "https://brandportal.shopee.com/seller/insights/sales/shop"
  
#   browser.execute_script("window.open('" + url + "');")
  
#   time.sleep(5)
#   browser.switch_to.window(browser.window_handles[-1])
  
#   # Select Customize datepicker
#   # Click Date range
#   browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[1]/div[2]/span/span/div/span[2]").click()
#   time.sleep(3)
  
#   # hover to customize
#   a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
#   actions.move_to_element(a).click().perform()
#   time.sleep(2)
  
#   select_FromTo_date(browser, from_date, to_date)
  
#   terminals = ["Gross_Data", "Net_Data"]
#   for term in terminals:
#     time.sleep(3)
#     # Click Terminal
#     browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[1]/div[3]/div/div").click()
    
#     if term == 'Gross_Data':
#       time.sleep(3)
#       print("Clicked 'Gross Data'! from Terminal")
#       browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[1]/div").click()
#     else:
#       time.sleep(3)
#       print("Clicked 'Net Data'! from Terminal")
#       browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]/div").click()
      
#     try:
#       time.sleep(3)
#       browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[2]").click()
#       print("Refresh data!")
#     except:
#       browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[2]/span").click()
#       print("Apply clicked")
      
#     print("Ready to download!")
#     time.sleep(10)
    
#     browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[3]").click()
    
#     print("Exporting data ...")
#     time.sleep(30)
    
#   print("Download finished for sales shop")

  
def get_sales_shop_data(browser,from_date, to_date):
    url = "https://brandportal.shopee.com/seller/insights/sales/shop"
    
    browser.execute_script("window.open('" + url + "');")
    
    time.sleep(5)
    browser.switch_to.window(browser.window_handles[-1])
    
    # Select Customize datepicker
    # Click Date range
    browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[1]/div[2]/span/span/div/span[2]").click()
    time.sleep(3)
    
    # hover to customize
    a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
    actions.move_to_element(a).click().perform()
    time.sleep(2)
    
    select_FromTo_date(browser, from_date, to_date)
    
    terminals = ["Gross_Data", "Net_Data"]
    for term in terminals:
        time.sleep(3)
        # Click Terminal
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[1]/div[3]/div/div").click()
        
        if term == 'Gross_Data':
            time.sleep(3)
            print("Clicked 'Gross Data'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[1]/div").click()
                                           
        else:
            time.sleep(3)
            print("Clicked 'Net Data'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]/div").click()            
        try:
            time.sleep(3)
            browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[2]").click()
            print("Refresh data!")
        except:
            browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[2]/span").click()
            print("Apply clicked")
            
        print("Ready to download!")
        time.sleep(10)
        
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[3]").click()
        
        print("Exporting data ...")
        time.sleep(30)
        
#         filepath = '/tmp/Shopee/sales/'        
#         filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
#         print(filen_name)
#         os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')
#         time.sleep(10)
                
    print("Download finished for sales shop")

# COMMAND ----------

def get_sales_product_performance_data(browser,from_date, to_date):
  
  url = "https://brandportal.shopee.com/seller/insights/product/performance"
  
  browser.execute_script("window.open('" + url + "');")
  
  time.sleep(5)
  browser.switch_to.window(browser.window_handles[-1])
  
  # Select Customize datepicker
  # Click Date range
  browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[1]/div[4]/span/span/div/span[2]").click()
  time.sleep(3)
  
  # hover to customize
  a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
  actions.move_to_element(a).click().perform()
  time.sleep(2)
  
  select_FromTo_date(browser, from_date, to_date)
  
  try:
    time.sleep(3)
    browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[2]").click()#span[2]
    print("Refresh data!")
  except:
    browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[2]").click()#span
    print("Apply clicked")
    
  print("Ready to download!")
  time.sleep(10)
    
  browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[3]").click()
  print("Exporting data ...")
  time.sleep(30)
        
#     filepath = '/tmp/Shopee/Marketing_solutions/'        
#     filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
#     print(filen_name)
#     os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')
#     time.sleep(10)
                
  print("Download finished for sales Product Performance")

# COMMAND ----------

def get_flash_sale_performance_data(browser,from_date, to_date):
  
  url = "https://brandportal.shopee.com/seller/insights/marketing/flashsale"
  
  browser.execute_script("window.open('" + url + "');")
  
  time.sleep(5)
  browser.switch_to.window(browser.window_handles[-1])
  
  # Select Customize datepicker
  # Click Date range
  browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[1]/div[4]/span/span/div/span[2]").click()
  time.sleep(3)
  
  # hover to customize
  a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[9]/div')
  actions.move_to_element(a).click().perform()
  time.sleep(2)
  
  select_FromTo_date(browser, from_date, to_date)
  
  terminals = ["Gross_Data", "Net_Data"]
  for term in terminals:
    time.sleep(3)
    # Click Terminal
    browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[1]/div[6]/div/div/div/span[2]").click()
    
    if term == 'Gross_Data':
      time.sleep(3)
      print("Clicked 'Gross Data'! from Terminal")
      browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[1]/div").click()
    else:
      time.sleep(3)
      print("Clicked 'Net Data'! from Terminal")
      browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]/div").click() 
      
      try:
        time.sleep(3)
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div/div/div/div[2]/button[2]").click()
        print("Refresh data!")
      except:
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div/div/div/div[2]/button[2]/span").click()
        print("Apply clicked")
        
      print("Ready to download!")
      time.sleep(10)
      
      browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div/div/div/div[2]/button[3]").click()
      
      print("Exporting data ...")
      time.sleep(30)
      
  print("Download finished for Flash Sale Performance")

# COMMAND ----------

