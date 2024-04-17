# Databricks notebook source
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_element(driver, element_tuple):
    return driver.find_element(element_tuple[0], element_tuple[1])

def wait_for_element_located(driver, element_tuple):
    element_present = EC.visibility_of_element_located((element_tuple[0], element_tuple[1]))
    WebDriverWait(driver, 30).until(element_present)

def wait_for_iframe(driver, frame_id):
    element_present = EC.frame_to_be_available_and_switch_to_it(frame_id)
    WebDriverWait(driver, 30).until(element_present)

def wait_for_invisible(driver, elemment_id):
    element_present = EC.invisibility_of_element_located((By.XPATH,elemment_id))
    WebDriverWait(driver, 30).until(element_present)