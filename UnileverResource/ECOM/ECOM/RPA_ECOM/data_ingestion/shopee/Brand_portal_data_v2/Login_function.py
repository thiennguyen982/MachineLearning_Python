# Databricks notebook source
# # Login function
# def login_ISR(driver, options, timeout, credentials):
#     check = None
#     while check is None:
#         try:
#             WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
#             time.sleep(2)
#             driver.find_element(By.ID, "email").clear()
#             driver.find_element(By.ID, "email").send_keys(credentials['email'])
#             time.sleep(2)
#             # driver.find_element_by_id('email').send_keys(credentials['email'])
#             driver.find_element(By.XPATH, "//*[@id='password']").clear()
#             driver.find_element(By.XPATH, "//*[@id='password']").send_keys(credentials['pass'])
#             time.sleep(2)
#             driver.find_element_by_xpath("//*[@id='app']/div/div/div/div/div[2]/div/div/div[3]/form/div[3]/div/div/button").click()
            
#             WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button")))
#             browser.find_element_by_xpath("//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button").click() ### Click GO
#             check = 'Sucessfully login!'
#             print(check)
#         except:
#             print("Cannot access! Browser closing ...")
#             browser.close()
#             browser.quit()

# COMMAND ----------

# # Login function
# def login_ISR(driver, options, timeout, credentials):
#     check = None
#     while check is None:
#         try:
#             WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
#             time.sleep(2)
#             driver.find_element_by_id("email").clear()
#             driver.find_element_by_id("email").send_keys(credentials['email'])
#             time.sleep(2)
#             # driver.find_element_by_id('email').send_keys(credentials['email'])
#             driver.find_element_by_xpath("//*[@id='password']").clear()
#             driver.find_element_by_xpath("//*[@id='password']").send_keys(credentials['pass'])
#             time.sleep(2)
#             driver.find_element_by_xpath("//*[@id='app']/div/div/div/div/div[2]/div/div/div[3]/form/div[3]/div/div/button").click()
            
#             WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button")))
#             browser.find_element_by_xpath("//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button").click() ### Click GO
#             check = 'Sucessfully login!'
#             print(check)
#         except:
#             print("Cannot access! Browser closing ...")
#             browser.close()
#             browser.quit()

# COMMAND ----------

login_button = "//*[@id='app']/div/div/div/div/div[2]/div/div/div[3]/form/div[3]/div/div/button"
go_button = "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button"

# COMMAND ----------

# Login function
def login_ISR(driver, options, timeout, credentials):
    check = None
    while check is None:
        try:
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
            driver.find_element(By.ID,"email").clear()
            driver.find_element(By.ID,"email").send_keys(credentials['email'])
            # driver.find_element(By.ID,'email').send_keys(credentials['email'])
            driver.find_element(By.XPATH,"//*[@id='password']").clear()
            driver.find_element(By.XPATH,"//*[@id='password']").send_keys(credentials['pass'])
            driver.find_element(By.XPATH,"//*[@id='app']/div/div/div/div/div[2]/div/div/div[3]/form/div[3]/div/div/button").click()
            
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button")))
            browser.find_element(By.XPATH,"//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button").click() ### Click GO
            check = 'Sucessfully login!'
            print(check)
        except:
            print("Cannot access! Browser closing ...")
            browser.close()
            browser.quit()

# COMMAND ----------

