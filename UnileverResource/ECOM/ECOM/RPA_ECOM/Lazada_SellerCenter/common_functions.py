# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../utilities/selenium_utilities

# COMMAND ----------

from selenium.webdriver.common.action_chains import ActionChains

# Login function
def login_ISR(driver, options, timeout, credentials):
    try:
        print('Waiting for locating input')
        wait_for_element_located(driver, EMAIL_INPUT_ELEMENT)
        get_element(driver, EMAIL_INPUT_ELEMENT).click()
        get_element(driver, EMAIL_INPUT_ELEMENT).clear()
        print('Sending Email')
        get_element(driver, EMAIL_INPUT_ELEMENT).send_keys(credentials['email'])
        # driver.find_element_by_id('email').send_keys(credentials['email'])
        get_element(driver, PASSWORD_INPUT_ELEMENT).click()
        get_element(driver, PASSWORD_INPUT_ELEMENT).clear()
        print('Sending Password')
        get_element(driver, PASSWORD_INPUT_ELEMENT).send_keys(credentials['pass'])
        wait_for_element_located(driver, LOGIN_BUTTON)

        a = ActionChains(browser)
        #identify element
        m = get_element(browser, LOGIN_BUTTON)
        #hover over element
        a.move_to_element(m).perform()
        print('Clicking Login')
        get_element(driver, LOGIN_BUTTON).click()
    except Exception as e:
        print(e)
        print("Cannot access! Browser closing ...")
        browser.close()

# COMMAND ----------

import time
from selenium.common.exceptions import TimeoutException

def download_data(browser, pre_url, from_date, to_date, date_type, store_path, file_name_pattern):
    print(f'Downloading data from {pre_url}: {from_date} to {to_date}')
    try:
      random_mouse_mover(browser)
      url = f'{pre_url}?dateType={date_type}&dateRange={from_date.strftime("%Y-%m-%d")}%7C{to_date.strftime("%Y-%m-%d")}'
      print('Loading URL...')
      browser.set_page_load_timeout(10)
      try:
        browser.get(url)
      except TimeoutException as timeout_exception:
        print("Page load Timeout Occurred. Quitting !!!")
        browser.quit()
        raise timeout_exception
      wait_for_element_located(browser, EXPORT_BUTTON)
      pre_export = ActionChains(browser)
      pre_export.move_by_offset(10, 20).perform()
      time.sleep(2)
      export_element = get_element(browser, EXPORT_BUTTON)
      pre_export.move_to_element(export_element).perform()
      print('Clicking export...')
      get_element(browser, EXPORT_BUTTON).click()
      
      wait_for_element_located(browser, OK_EXPORT_BUTTON)
      a = ActionChains(browser)
      #identify element
      m = get_element(browser, OK_EXPORT_BUTTON)
      #hover over element
      a.move_to_element(m).perform()
      print('Clicking Ok...')
      get_element(browser, OK_EXPORT_BUTTON).click()
      # check if downloading is done
#       file_name = file_name_pattern % (from_date.strftime('%Y.%m.%d'), to_date.strftime('%Y.%m.%d'))
      retry = 20
      i = 0
      while (len(dbutils.fs.ls(f"file:{store_path}")) == 0 or dbutils.fs.ls(f'file:{store_path}')[0].path.split('.')[1] != 'xls') and i < retry:
          print('Downloading...')
          time.sleep(5)
          i += 1
      time.sleep(5)
      return dbutils.fs.ls(f'file:{store_path}')[0].path
    except Exception as e:
      raise e

# COMMAND ----------

# random mouse move
import numpy as np
import scipy.interpolate as si

def random_mouse_mover(driver):
  # Curve base:
  points = [[0, 0], [0, 2], [2, 3], [4, 0], [6, 3], [8, 2], [8, 0]];
  points = np.array(points)

  x = points[:,0]
  y = points[:,1]


  t = range(len(points))
  ipl_t = np.linspace(0.0, len(points) - 1, 100)

  x_tup = si.splrep(t, x, k=3)
  y_tup = si.splrep(t, y, k=3)

  x_list = list(x_tup)
  xl = x.tolist()
  x_list[1] = xl + [0.0, 0.0, 0.0, 0.0]

  y_list = list(y_tup)
  yl = y.tolist()
  y_list[1] = yl + [0.0, 0.0, 0.0, 0.0]

  x_i = si.splev(ipl_t, x_list) # x interpolate values
  y_i = si.splev(ipl_t, y_list) # y interpolate values

  action =  ActionChains(driver);

  startElement = get_element(driver, FIRST_DIV)

  # First, go to your start point or Element:
  action.move_to_element(startElement);
  action.perform();

  print('Performing random move...')
  for mouse_x, mouse_y in zip(x_i, y_i):
    action.move_by_offset(mouse_x,mouse_y);
    action.perform();
  print('End random move...')