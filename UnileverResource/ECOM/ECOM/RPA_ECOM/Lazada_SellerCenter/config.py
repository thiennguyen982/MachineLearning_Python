# Databricks notebook source
# Databricks notebook source
# Constant folder
PRODUCT_PERFORMANCE_TMP_DIR = '/tmp/Lazada_BOT_ProductPerformance/'

# COMMAND ----------

from selenium.webdriver.common.by import By

EMAIL_INPUT_ELEMENT = (By.ID, 'account')
PASSWORD_INPUT_ELEMENT = (By.ID,'password')
LOGIN_BUTTON = (By.XPATH, '//button[contains(@class, "next-btn")]/span[contains(@class, "next-btn-helper") and contains(., "Đăng nhập")]/ancestor::button')

DA_PAGE = (By.XPATH, '//span[contains(., "Phân tích dữ liệu")]')

EXPORT_BUTTON = (By.XPATH, '//div[contains(@class, "ZfMh8w")]')
OK_EXPORT_BUTTON = (By.XPATH, '//button[contains(@class, "ant-btn")]/span[contains(., "Ok")]/ancestor::button')

FIRST_DIV = (By.XPATH, '//div[1]')