# Databricks notebook source
def product_performance(browser, date_range, year,month_name):
  print('entered into product_performance')
  try:
    tab_elements = browser.find_elements_by_class_name('text')
    for tab_element in tab_elements:
      #print(tab_element.text)
      if tab_element.text=="Product":
        tab_element.click()
        print('clicked on product tab')
        break
        
    time.sleep(5)
    pro_elements = browser.find_elements_by_class_name('name')
    for pro_element in pro_elements:
      #print(pro_element.text)
      if pro_element.text=="Product Performance":
        pro_element.click()
        print('clicked on Product Performance')
        break
        
    #click on date range
    time.sleep(8)
    date_range_xpath = "//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span"
    button = browser.find_element(by=By.XPATH, value=date_range_xpath)
    browser.implicitly_wait(10)
    ActionChains(browser).move_to_element(button).click(button).perform()
    print('clicked on date range')    
    time.sleep(4)
    
    if date_range == 'Yesterday':
      Yesterday = "/html/body/div[4]/div/div/ul/li[2]/span[1]"                   
      browser.find_element(by=By.XPATH, value=Yesterday).click()
      print('clicked on Yesterday')  
    
    elif date_range == 'Past 7 Days':
      last_7_days = "/html/body/div[4]/div/div/ul/li[3]/span[1]"
      browser.find_element(by=By.XPATH, value=last_7_days).click()
      print('clicked on last 7 days')
      
    elif date_range == 'Past 30 Days':
      last_30_days = "/html/body/div[4]/div/div/ul/li[4]/span[1]"
      browser.find_element(by=By.XPATH, value=last_30_days).click()
      print('clicked on last 30 days')
      
    else:    
      time.sleep(2)
      By_month = "/html/body/div[4]/div/div/ul/li[8]/span"
      # hover to month
      a = browser.find_element(by=By.XPATH, value=By_month)
      actions.move_to_element(a).click().perform()
      print('hovered on By Month')
      
      time.sleep(5)
      months_elements = browser.find_elements_by_class_name('shopee-month-table__col')
      for month_element in months_elements:
        #print(month_element.text)
        if month_element.text == month_name:
          month_element.click()
          print('clicked on Month')
          break
    
    try:
      time.sleep(8)
      export_xpath = "/html/body/div[1]/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div[1]/div/button"
      export_button = browser.find_element(by=By.XPATH, value=export_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(export_button).click(export_button).perform()
      print('clicked on Export Data')
    except:
      print('Error in clicking on Export button')
      
    try:
      time.sleep(8)
      down_xpath = "//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div[1]/div/div/div[2]/div/div/div[3]/div[3]/div[1]/div/div/div[2]/table/tbody/tr[1]/td[2]/div/div/button"
      down_button = browser.find_element(by=By.XPATH, value=down_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(down_button).click(down_button).perform()
      print('clicked on download button')
      print('Sucessfully download the Product Performance Dataset')
    except:
      print('Error in clicking on download button')
  
  except:
    print('Error in downloading the Product Performance Dataset')

# COMMAND ----------

def traffic_overview(browser, date_range, year,month_name):
  print('entered into traffic_overview')
  try:
    tab_elements = browser.find_elements_by_class_name('text')
    for tab_element in tab_elements:
      #print(tab_element.text)
      if tab_element.text=="Traffic":
        tab_element.click()
        print('clicked on Traffic tab')
        break
        
    time.sleep(5)
    pro_elements = browser.find_elements_by_class_name('name')
    for pro_element in pro_elements:
      #print(pro_element.text)
      if pro_element.text=="Traffic Overview":
        pro_element.click()
        print('clicked on Traffic Overview')
        break
        
    #click on date range
    time.sleep(8)
    date_range_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span"                     
    button = browser.find_element(by=By.XPATH, value=date_range_xpath)
    browser.implicitly_wait(10)
    ActionChains(browser).move_to_element(button).click(button).perform()
    print('clicked on date range')
    
    time.sleep(4) 
    if date_range == 'Yesterday':
      Yesterday = "/html/body/div[4]/div/div/ul/li[1]/span[1]"                     
      browser.find_element(by=By.XPATH, value=Yesterday).click()
      print('clicked on Yesterday')   
    
    elif date_range == 'Past 7 Days':
      last_7_days = "/html/body/div[4]/div/div/ul/li[2]/span[1]"                     
      browser.find_element(by=By.XPATH, value=last_7_days).click()
      print('clicked on last 7 days')
      
    elif date_range == 'Past 30 Days':
      last_30_days = "/html/body/div[4]/div/div/ul/li[3]/span[1]"                      
      browser.find_element(by=By.XPATH, value=last_30_days).click()
      print('clicked on last 30 days')
      
    else:    
      time.sleep(2)
      By_month = "/html/body/div[4]/div/div/ul/li[7]/span"                
      # hover to month
      a = browser.find_element(by=By.XPATH, value=By_month)
      actions.move_to_element(a).click().perform()
      print('hovered on By Month')
      
      time.sleep(5)
      months_elements = browser.find_elements_by_class_name('shopee-month-table__col')
      for month_element in months_elements:
        #print(month_element.text)
        if month_element.text == month_name:
          month_element.click()
          print('clicked on Month')
          break
    
    try:
      time.sleep(8)
      export_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div[3]/div/button"
      export_button = browser.find_element(by=By.XPATH, value=export_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(export_button).click(export_button).perform()
      print('clicked on Export Data')
      print('Sucessfully download the Traffic Overview Dataset')
    except:
      print('Error in clicking on Export button')
  
  except:
    print('Error in downloading the Traffic Overview Dataset')

# COMMAND ----------

def flash_sale(browser, date_range, year,month_name):
  print('entered into Flash Sale')
  try:
    tab_elements = browser.find_elements_by_class_name('text')
    for tab_element in tab_elements:
      #print(tab_element.text)
      if tab_element.text=="Marketing":
        tab_element.click()
        print('clicked on Marketing tab')
        break
        
    time.sleep(5)
    pro_elements = browser.find_elements_by_class_name('name')
    for pro_element in pro_elements:
      #print(pro_element.text)
      if pro_element.text=="My Shop's Flash Sale":
        pro_element.click()
        print('clicked on Flash Sale')
        break
        
    #click on date range
    time.sleep(8)
    date_range_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div/div[2]/div[1]/div[2]/div/div[1]/div/div/div/div[1]/div/span"
    button = browser.find_element(by=By.XPATH, value=date_range_xpath)
    browser.implicitly_wait(10)
    ActionChains(browser).move_to_element(button).click(button).perform()
    print('clicked on date range')
    
    time.sleep(4)
    
    if date_range == 'Yesterday':
      last_7_days = "/html/body/div[4]/div/div/ul/li[2]/span[1]"
      browser.find_element(by=By.XPATH, value=last_7_days).click()
      print('clicked on Yesterday')
      
    elif date_range == 'Past 7 Days':
      last_7_days = "/html/body/div[4]/div/div/ul/li[3]/span[1]"
      browser.find_element(by=By.XPATH, value=last_7_days).click()
      print('clicked on Past 7 Days')
      
    elif date_range == 'Past 30 Days':
      last_30_days = "/html/body/div[4]/div/div/ul/li[4]/span[1]"  
      browser.find_element(by=By.XPATH, value=last_30_days).click()
      print('clicked on last 30 days')
      
    else:    
      time.sleep(2)
      By_month = "/html/body/div[4]/div/div/ul/li[8]/span"
      # hover to month
      a = browser.find_element(by=By.XPATH, value=By_month)
      actions.move_to_element(a).click().perform()
      print('hovered on By Month')
      
      time.sleep(5)
      months_elements = browser.find_elements_by_class_name('shopee-month-table__col')
      for month_element in months_elements:
        #print(month_element.text)
        if month_element.text == month_name:
          month_element.click()
          print('clicked on Month')
          break
    
    try:
      time.sleep(8)
      export_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div/div[2]/div[1]/div[2]/div/button"
      export_button = browser.find_element(by=By.XPATH, value=export_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(export_button).click(export_button).perform()
      print('clicked on Export Data')
      print('Sucessfully download the Flash Sale Dataset')
    except:
      print('Error in clicking on Export button')
  
  except:
    print('Error in downloading the Flash Sale Dataset')

# COMMAND ----------

def Livestream(browser, date_range, year,month_name):
  print('entered into livesteam')
  try:
    tab_elements = browser.find_elements_by_class_name('text')
    for tab_element in tab_elements:
      #print(tab_element.text)
      if tab_element.text=="Marketing":
        tab_element.click()
        print('clicked on Marketing tab')
        break
        
    time.sleep(5)
    pro_elements = browser.find_elements_by_class_name('name')
    for pro_element in pro_elements:
      #print(pro_element.text)
      if pro_element.text=="Livestream":
        pro_element.click()
        print('clicked on livestream')
        break
        
    #click on date range
    time.sleep(8)
    date_range_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div/div[4]/div/div[1]/div/div/div/div[1]/div/span"                  
    button = browser.find_element(by=By.XPATH, value=date_range_xpath)
    browser.implicitly_wait(10)
    ActionChains(browser).move_to_element(button).click(button).perform()
    print('clicked on date range')
    
    time.sleep(4)
    
    if date_range == 'Past 7 Days':
      last_7_days = "/html/body/div[4]/div/div/ul/li[1]/span[1]"                     
      browser.find_element(by=By.XPATH, value=last_7_days).click()
      print('clicked on Past 7 Days')
      
    elif date_range == 'Past 30 Days':
      last_30_days = "/html/body/div[4]/div/div/ul/li[2]/span[1]"                      
      browser.find_element(by=By.XPATH, value=last_30_days).click()
      print('clicked on last 30 days')
      
    else:    
      time.sleep(2)
      By_month = "/html/body/div[4]/div/div/ul/li[6]/span"                
      # hover to month
      a = browser.find_element(by=By.XPATH, value=By_month)
      actions.move_to_element(a).click().perform()
      print('hovered on By Month')
      
      time.sleep(5)
      months_elements = browser.find_elements_by_class_name('shopee-month-table__col')
      for month_element in months_elements:
        #print(month_element.text)
        if month_element.text == month_name:
          month_element.click()
          print('clicked on Month')
          break
    
    try:
      time.sleep(8)
      export_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div/div[4]/div/button"
      export_button = browser.find_element(by=By.XPATH, value=export_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(export_button).click(export_button).perform()
      print('clicked on Export Data')
      print('Sucessfully download the livestream Dataset')
    except:
      print('Error in clicking on Export button')
  
  except:
    print('Error in downloading the livestream Dataset')