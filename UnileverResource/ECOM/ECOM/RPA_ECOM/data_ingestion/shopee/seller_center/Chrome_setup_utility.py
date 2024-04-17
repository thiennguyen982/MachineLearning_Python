# Databricks notebook source
!pip install webdriver_manager chromedriver_autoinstaller pandas

# COMMAND ----------

# MAGIC %sh cat /etc/*-release

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo add-apt-repository universe
# MAGIC sudo add-apt-repository multiverse

# COMMAND ----------

import requests
import selenium

def get_chrome_latest_release():
    url = "https://chromedriver.storage.googleapis.com/LATEST_RELEASE"
    response = requests.request("GET", url)
    return response.text

latest_chrome_version  = get_chrome_latest_release()
latest_chrome_version

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget https://chromedriver.storage.googleapis.com/103.0.5060.53/chromedriver_linux64.zip /tmp/chromedriver_linux64.zip
# MAGIC rm -r /tmp/chromedriver
# MAGIC mkdir /tmp/chromedriver
# MAGIC unzip /tmp/chromedriver_linux64.zip -d /tmp/chromedriver/
# MAGIC sudo add-apt-repository ppa:canonical-chromium-builds/stage
# MAGIC /usr/bin/yes | sudo apt update
# MAGIC /usr/bin/yes | sudo apt install chromium-browser

# COMMAND ----------

# MAGIC %sh rm -f google-chrome-stable_current_amd64.deb

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo killall apt-get apt dpkg
# MAGIC sudo apt --fix-broken install -y
# MAGIC sudo apt install -y libxss1 libappindicator1 libindicator7 fonts-liberation libgbm1 libnspr4 libnss3 libwayland-server0
# MAGIC wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# MAGIC

# COMMAND ----------

# MAGIC %sh sudo apt-get update -y
# MAGIC

# COMMAND ----------

# MAGIC %sh 
# MAGIC sudo dpkg -i ./google-chrome*.deb 

# COMMAND ----------

# MAGIC %sh google-chrome --product-version
# MAGIC

# COMMAND ----------

import chromedriver_autoinstaller
from selenium import webdriver
chrome_driver = "/tmp/chromedriver/chromedriver"

chromedriver_autoinstaller.install()


# COMMAND ----------

import subprocess

command = "chromedriver --version" 
completed_process = subprocess.run(command, shell=True, text=True, capture_output=True)
print(completed_process)
output = completed_process.stdout
output

# COMMAND ----------

import re
regex = "ChromeDriver (.*) .*"
chrome_version = re.search(regex, output).group(1)
print(chrome_version)

# COMMAND ----------

