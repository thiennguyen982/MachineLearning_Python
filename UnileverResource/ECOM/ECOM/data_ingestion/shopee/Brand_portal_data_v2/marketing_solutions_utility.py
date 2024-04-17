# Databricks notebook source
def get_campaign_performance_data(browser,from_date, to_date):
    print('entered in campaign')
    url = "https://brandportal.shopee.com/seller/mkt/traffic/campaign"
    
    browser.execute_script("window.open('" + url + "');")
    
    time.sleep(5)
    browser.switch_to.window(browser.window_handles[-1])
    
    # Select Customize datepicker
    # Click Date range
    browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[2]/div/div/div/div/span").click()
    time.sleep(3)
    
    # hover to customize
    a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
    actions.move_to_element(a).click().perform()
    time.sleep(2)
    
    select_FromTo_date(browser, from_date, to_date)
    
    terminals = ["App", "Web"]
    for term in terminals:
        time.sleep(3)
        # Click Terminal
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[3]").click()
        
        if term == 'App':
            time.sleep(3)
            print("Clicked 'App'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]").click()
        else:
            time.sleep(3)
            print("Clicked 'Web'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[3]").click()
            
        try:
            time.sleep(3)
            browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[2]").click()
            print("Refresh data!")
        except:
            browser.find_element_by_xpath("//*[@type='button'][@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
            print("Apply clicked")
            
        print("Ready to download!")
        time.sleep(10)
        
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
        
        print("Exporting data ...")
        time.sleep(30)
        
        filepath = '/tmp/Shopee/'        
        filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
        print(filen_name)
        os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')
        time.sleep(10)
                
    print("Download finished for campaign")

# COMMAND ----------

def get_product_performance_data(browser,from_date, to_date):
    print('entered in product')
    time.sleep(5)
    url = "https://brandportal.shopee.com/seller/mkt/traffic/product"
    
    browser.execute_script("window.open('" + url + "');")
    
    time.sleep(5)
    browser.switch_to.window(browser.window_handles[-1])
    
    print("Clicked date Period")
    browser.find_element(by=By.XPATH, value='//*[@id="app"]/div/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[2]/div/form/div/div[2]/div/div/div/div/span').click()
    time.sleep(2)
    
    # hover to customize
    a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
    actions.move_to_element(a).click().perform()
    time.sleep(2)
    
    select_FromTo_date(browser, from_date, to_date)
    
    terminals = ["App", "Web"]
    for term in terminals:
        time.sleep(3)
        # Click Terminal
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[3]").click()
        
        if term == 'App':
            time.sleep(3)
            print("Clicked 'App'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]").click()
            
        else:
            time.sleep(3)
            print("Clicked 'Web'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[3]").click()
            
        try:
            time.sleep(3)
            browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[2]").click()
            print("Refresh data!")
        except:
            browser.find_element_by_xpath("//*[@type='button'][@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
            print("Apply clicked")
            
        print("Ready to download!")
        
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
        
        print("Exporting data ...")
        time.sleep(30)
        
        filepath = '/tmp/Shopee/'
        filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
        print(filen_name)
        os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')   
        time.sleep(8)    
        
    print("Download finished for Product")

# COMMAND ----------

# def get_product_performance_data(browser,from_date, to_date):
#     print('entered in product')
#     time.sleep(5)
#     url = "https://brandportal.shopee.com/seller/mkt/traffic/product"
    
#     browser.execute_script("window.open('" + url + "');")
    
#     time.sleep(5)
#     browser.switch_to.window(browser.window_handles[-1])
    
#     print("Clicked date Period")
#     browser.find_element(by=By.XPATH, value='//*[@id="app"]/div/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[2]/div/form/div/div[2]/div/div/div/div/span').click()
#     time.sleep(2)
    
#     # hover to customize
#     a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
#     actions.move_to_element(a).click().perform()
#     time.sleep(2)
    
#     select_FromTo_date(browser, from_date, to_date)
    
#     terminals = ["App", "Web"]
#     for term in terminals:
#         time.sleep(3)
#         # Click Terminal
#         browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[3]").click()
        
#         if term == 'App':
#             time.sleep(3)
#             print("Clicked 'App'! from Terminal")
#             print('ok')
#             try:
#               browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]/div").click()
#                                              #/html/body/div[2]/div/div/div/div[2]/div/div/div[2]/div
#             except:
#               print('Error')
#                                            #/html/body/div[2]/div/div/div/div[2]/div/div/div[2]/div
#             print('clicked')
            
#         else:
#             time.sleep(3)
#             print("Clicked 'Web'! from Terminal")
#             browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[3]").click()
            
#         try:
#             time.sleep(3)
#             print('clicking on refresh')
#             browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[2]").click()
#             print("Refresh data!")
#         except:
#             browser.find_element_by_xpath("//*[@type='button'][@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
#             print("Apply clicked")
            
#         print("Ready to download!")
        
#         browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
        
#         print("Exporting data ...")
#         time.sleep(30)
        
#         filepath = '/tmp/Shopee/'
#         filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
#         print(filen_name)
#         os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')   
#         time.sleep(8)    
        
#     print("Download finished for Product")

# COMMAND ----------

# def get_product_performance_data(browser,from_date, to_date):
#     print('entered in product')
#     time.sleep(5)
#     url = "https://brandportal.shopee.com/seller/mkt/traffic/product"
    
#     browser.execute_script("window.open('" + url + "');")
    
#     time.sleep(5)
#     browser.switch_to.window(browser.window_handles[-1])
    
#     print("Clicked date Period")
#     browser.find_element(by=By.XPATH, value='//*[@id="app"]/div/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[2]/div/form/div/div[2]/div/div/div/div/span').click()
#     time.sleep(2)
    
#     # hover to customize
#     a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
#     actions.move_to_element(a).click().perform()
#     time.sleep(2)
    
#     select_FromTo_date(browser, from_date, to_date)
    
#     terminals = ["App", "Web"]
#     for term in terminals:
#         time.sleep(3)
#         # Click Terminal
#         browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[3]").click()
        
#         if term == 'App':
#             time.sleep(7)
#             print("Clicked 'App'! from Terminal")
#             print('ok1')
#             browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]/div").click()
#                                            #/html/body/div[4]/div/div/div/div[2]/div/div/div[2]/div
#                                              #/html/body/div[2]/div/div/div/div[2]/div/div/div[2]/div
#            # print('Error')
#                                            #/html/body/div[2]/div/div/div/div[2]/div/div/div[2]/div
#            # print('clicked')
            
#         else:
#             time.sleep(3)
#             print("Clicked 'Web'! from Terminal")
#             browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[3]").click()
            
#         try:
#             time.sleep(3)
#             print('clicking on refresh')
#             browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[2]").click()
#             print("Refresh data!")
#         except:
#             browser.find_element_by_xpath("//*[@type='button'][@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
#             print("Apply clicked")
            
#         print("Ready to download!")
        
#         browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
        
#         print("Exporting data ...")
#         time.sleep(40)
        
#         filepath = '/tmp/Shopee/'
#         filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
#         print(filen_name)
#         os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')   
#         time.sleep(8)    
        
#     print("Download finished for Product")

# COMMAND ----------

