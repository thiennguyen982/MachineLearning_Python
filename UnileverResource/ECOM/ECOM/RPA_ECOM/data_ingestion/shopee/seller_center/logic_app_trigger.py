# Databricks notebook source
#time.sleep(90)
def get_OTP():
  schema = { "otp": "not fetched" }
  otp= requests.post("https://prod-108.westeurope.logic.azure.com:443/workflows/3c940823adef4679b882b6c732458b49/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=EzIKXAZZgdVuxmrlzdPzx-uHiMJqq7QiMwV7uUVAni8",json=schema)  
  final_otp = str(otp.text)[8:14]
  print(final_otp)
  return final_otp