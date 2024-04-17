# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../utilities/selenium_utilities

# COMMAND ----------

# Login function
def login_ISR(driver, options, timeout, credentials):
  try:
    wait_for_element_located(driver, (By.ID, 'app'))
    get_element(driver, EMAIL_INPUT_ELEMENT).clear()
    print('Sending Email')
    get_element(driver, EMAIL_INPUT_ELEMENT).send_keys(credentials['email'])
    # driver.find_element_by_id('email').send_keys(credentials['email'])
    get_element(driver, PASSWORD_INPUT_ELEMENT).clear()
    print('Sending Password')
    get_element(driver, PASSWORD_INPUT_ELEMENT).send_keys(credentials['pass'])
    print('Clicking Login')
    get_element(driver, LOGIN_BUTTON).click()

    wait_for_element_located(driver, PRINCIPAL_GO_BUTTON)
    print('Click GO')
    get_element(driver, PRINCIPAL_GO_BUTTON).click()  # Click GO
    check = 'Sucessfully login!'
    print(check)
  except Exception as e:
    print("Cannot access! Browser closing ...")
    browser.close()
    browser.quit()
    raise e

# COMMAND ----------

from datetime import datetime
from selenium.webdriver.common.action_chains import ActionChains

def select_date(date):
    year = date.strftime('%Y')
    month = date.strftime('%b')
    day = date.strftime('%#d')
    print(f'Select date {day}-{month}-{year}')
    wait_for_element_located(browser, YEAR_PICKER_ELEMENT)
    get_element(browser, YEAR_PICKER_ELEMENT).click()
    wait_for_element_located(browser, (By.XPATH, CELL_ELEMENT_FORMAT % year))
    print(f'Clicking year {year}')
    get_element(browser, (By.XPATH, CELL_ELEMENT_FORMAT % year)).click()
    wait_for_element_located(browser, MONTH_PICKER_ELEMENT)
    get_element(browser, MONTH_PICKER_ELEMENT).click()
    wait_for_element_located(browser, (By.XPATH, CELL_ELEMENT_FORMAT % f'"{month}"'))
    print(f'Clicking month {month}')
    get_element(browser, (By.XPATH, CELL_ELEMENT_FORMAT % f'"{month}"')).click()
    wait_for_element_located(browser, (By.XPATH, CELL_ELEMENT_FORMAT % day))
    print(f'Clicking date {day}')
    get_element(browser, (By.XPATH, CELL_ELEMENT_FORMAT % day)).click()
    print(f'Selected date at {day}-{month}-{year}')

def select_date_range(from_date, to_date):
    wait_for_element_located(browser, RESET_DATE_RANGE_BUTTON)
    # reset date range
    print('Clicking Reset date range')
    get_element(browser, RESET_DATE_RANGE_BUTTON).click()
    # select date picker
    print('Clicking Date picker...')
    get_element(browser, DATE_PICKER_ELEMENT).click()
    wait_for_element_located(browser, CUSTOMIZE_DATE_PICKER_ELEMENT)
    print('Clicking Customize...')
    get_element(browser, CUSTOMIZE_DATE_PICKER_ELEMENT).click()
    select_date(from_date)
    # move hover to yesterday
    a = ActionChains(browser)
    #identify element
    m = get_element(browser, YESTERDAY_DATE_PICKER_ELEMENT)
    #hover over element
    a.move_to_element(m).perform()
    get_element(browser, CUSTOMIZE_DATE_PICKER_ELEMENT).click()
    select_date(to_date)

# COMMAND ----------

import time
from Screenshot import Screenshot

def download_data(from_date, to_date, store_path, file_name_pattern):
    print(f'Downloading data from {from_date} to {to_date}')
    try:
      select_date_range(from_date, to_date)
      wait_for_element_located(browser, (By.XPATH, REFRESH_APPLY_BUTTON_PATTERN % '"Refresh"'))
      print('Clicking Refresh...')
      get_element(browser, (By.XPATH, REFRESH_APPLY_BUTTON_PATTERN % '"Refresh"')).click()
      wait_for_element_located(browser, (By.XPATH, REFRESH_APPLY_BUTTON_PATTERN % '"Apply"'))
      print('Clicking export...')
      get_element(browser, EXPORT_BUTTON).click()
      # check if downloading is done
      file_name = file_name_pattern % (from_date.strftime('%Y.%m.%d'), to_date.strftime('%Y.%m.%d'))
      retry = 5
      i = 0
      while not path_exists(f'file:{store_path}{file_name}') and i < retry:
          print(f'Downloading... - Dest: file:{store_path}{file_name}')
          time.sleep(5)
          i += 1
      return f'file:{store_path}{file_name}' if path_exists(f'file:{store_path}{file_name}') else None
    except Exception as e:
      ob = Screenshot.Screenshot()
      img_url = ob.full_Screenshot(browser, save_path=store_path,image_name="screenshot.jpg")
      dbutils.fs.cp(f"file:{store_path}screenshot.jpg", f"dbfs:/FileStore/Shopee_screenshot{datetime.now().strftime('%Y%m%d-%H%M%S')}.jpg")
      raise e