# Databricks notebook source
# Constant folder
SALE_PERFORMANCE_TMP_DIR = '/tmp/Shopee_BOT_SalesPerformance/'
SALE_PERFORMANCE_FILE_PATTERN = 'Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-%s_%s.xlsx'

PRODUCT_PERFORMANCE_TMP_DIR = '/tmp/Shopee_BOT_ProductPerformance/'
PRODUCT_PERFORMANCE_FILE_PATTERN = 'Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-%s_%s.xlsx'

# COMMAND ----------

from selenium.webdriver.common.by import By

EMAIL_INPUT_ELEMENT = (By.ID, 'email')
PASSWORD_INPUT_ELEMENT = (By.ID,'password')
LOGIN_BUTTON = (By.XPATH, '//button[contains(@class, "src-pages-login---LoginBtn--1oecp")]')
PRINCIPAL_GO_BUTTON = (By.XPATH, '//button[contains(@class, "src-pages-postLogin---button--2nu7A")]')

RESET_DATE_RANGE_BUTTON = (By.XPATH, '//button[contains(@class,"track-click-global-filter-reset")]/span[contains(.,"Reset")]/ancestor::button')
DATE_PICKER_ELEMENT = (By.XPATH, '//div[contains(@class, "src-components_v1-DatePicker---datePicker--2-8s4")]')
YESTERDAY_DATE_PICKER_ELEMENT = (By.XPATH, '//span[contains(@class, "shopee-react-date-picker-shortcut-item__text") and contains(., "Yesterday")]//ancestor::li')
CUSTOMIZE_DATE_PICKER_ELEMENT = (By.XPATH, '//span[contains(@class, "shopee-react-date-picker-shortcut-item__text") and contains(., "Customize")]//ancestor::li')
YEAR_PICKER_ELEMENT = (By.XPATH, '//div[contains(@class, "shopee-react-date-picker__header")]/span[contains(@class, "date-box")]/span[contains(@class, "date-default-style year")]')
MONTH_PICKER_ELEMENT = (By.XPATH, '//div[contains(@class, "shopee-react-date-picker__header")]/span[contains(@class, "date-box")]/span[contains(@class, "date-default-style month")]')
CELL_ELEMENT_FORMAT = '//div[contains(@class,"shopee-react-date-picker__table-cell") and contains(.,%s)]/div[not(contains(@class,"out-of-range"))]'
REFRESH_APPLY_BUTTON_PATTERN = '//button[contains(@class, "track-click-global-filter-refresh")]/span[contains(.,%s)]/ancestor::button'
EXPORT_BUTTON = (By.XPATH, '//button[contains(@class, "track-click-global-filter-export")]')