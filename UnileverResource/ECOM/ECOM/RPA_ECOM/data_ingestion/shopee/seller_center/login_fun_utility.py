# Databricks notebook source
# Login function
def login_ISR(driver, options, timeout, credentials):
  check = None
  while check is None:
    try:
      
      #WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
      #filling the email id
      print('entered into login')
      userId_xpath = "/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input"
      driver.find_element(by=By.XPATH, value=userId_xpath).clear()
      driver.find_element(by=By.XPATH, value=userId_xpath).send_keys(credentials['email'])
      time.sleep(2)
      print('filled email')
      
      #filling password
      password_xpath = "/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[2]/div/div/input"
      driver.find_element(by=By.XPATH, value=password_xpath).clear()
      driver.find_element(by=By.XPATH, value=password_xpath).send_keys(credentials['pass'])
      time.sleep(2)
      print('filled password')
      
      #hit login button
      logIn_button_xpath = "/html/body/div/main/div/div[1]/div/div/div/div/div/div/button[2]"
      driver.find_element(by=By.XPATH, value=logIn_button_xpath).click()
      print('clicked on login button!')
      check = 'Sucessfully login!'
      print('Sucessfully login!')
      
    except:
      print("Cannot access! Browser closing ...")
      browser.close()
      browser.quit()