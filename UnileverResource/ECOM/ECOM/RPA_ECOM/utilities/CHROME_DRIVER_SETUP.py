# Databricks notebook source
# MAGIC %run ./file_ultilities

# COMMAND ----------

# Common functions
import requests

def get_chrome_latest_release():
    url = "https://chromedriver.storage.googleapis.com/LATEST_RELEASE"
    response = requests.request("GET", url)
    return response.text

def download_file(url, save_path):
    dbfs_save_path = f'/dbfs/{save_path}'
    with requests.get(url) as r:
        r.raise_for_status()
        with open(dbfs_save_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024): 
                f.write(chunk)
    return save_path

# chrome_version = get_chrome_latest_release()
chrome_version = '106.0.5249.61'
print(f'Current Chrome version is {chrome_version}')

# COMMAND ----------

def get_chrome_installer_download_url(version):
    chrome_url_download=f'https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_{version}_amd64.deb'
    response = requests.head(chrome_url_download)
    if response.status_code == 404:
        chrome_url_download=f'https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_{version}-1_amd64.deb'
    return chrome_url_download

def get_driver_download_url(version):
    return f'https://chromedriver.storage.googleapis.com/{version}/chromedriver_linux64.zip'


LIB_DIR = '/mnt/databricks'
LOCAL_CHROME_INSTALLER_PATH = '/tmp/google-chrome-stable_amd64.deb'
BLOB_CHROME_INSTALLER_PATH = f'{LIB_DIR}/google-chrome-stable_{chrome_version}_amd64.deb'

LOCAL_DIRVER_PATH = '/tmp/chromedriver.zip'
BLOB_DIRVER_PATH = f'{LIB_DIR}/chromedriver_{chrome_version}.zip'
# check if the Chrome installer has been downloaded
if path_exists(BLOB_CHROME_INSTALLER_PATH):
    print(f'Copying installer to local at path {LOCAL_CHROME_INSTALLER_PATH}')
    dbutils.fs.cp(BLOB_CHROME_INSTALLER_PATH, LOCAL_CHROME_INSTALLER_PATH)
else:
    chrome_url_download = get_chrome_installer_download_url(chrome_version)
    # Download Chrome
    print(f'Downloading Chrome installer at url {chrome_url_download}')
    download_file(chrome_url_download, LOCAL_CHROME_INSTALLER_PATH)
    print(f'Storing installer to blob storage')
    dbutils.fs.cp(LOCAL_CHROME_INSTALLER_PATH, BLOB_CHROME_INSTALLER_PATH)

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/ -l

# COMMAND ----------

# MAGIC %sh
# MAGIC # install Chrome
# MAGIC sudo apt remove -y google-chrome-stable
# MAGIC sudo dpkg -i /dbfs/tmp/google-chrome-stable_amd64.deb
# MAGIC sudo apt-get update
# MAGIC sudo apt-get install -y -f --fix-missing

# COMMAND ----------

# check if the Chrome installer has been downloaded
if path_exists(BLOB_DIRVER_PATH):
    print(f'Copying installer to local at path {LOCAL_DIRVER_PATH}')
    dbutils.fs.cp(BLOB_DIRVER_PATH, LOCAL_DIRVER_PATH)
else:
    driver_url_download = get_driver_download_url(chrome_version)
    # Download Chrome
    print(f'Downloading Chrome driver at url {driver_url_download}')
    download_file(driver_url_download, LOCAL_DIRVER_PATH)
    print(f'Storing installer to blob storage')
    dbutils.fs.cp(LOCAL_DIRVER_PATH, BLOB_DIRVER_PATH)

# COMMAND ----------

# MAGIC %sh
# MAGIC # extract Driver
# MAGIC rm -r /tmp/chromedriver
# MAGIC mkdir /tmp/chromedriver
# MAGIC unzip /dbfs/tmp/chromedriver.zip -d /tmp/chromedriver/

# COMMAND ----------

dbutils.notebook.exit('/tmp/chromedriver/chromedriver')