# Databricks notebook source
# MAGIC %sh wget https://chromedriver.storage.googleapis.com/90.0.4430.24/chromedriver_linux64.zip -O /tmp/chromedriver_linux64.zip

# COMMAND ----------

# MAGIC %sh rm -r /tmp/chromedriver

# COMMAND ----------

# MAGIC %sh mkdir /tmp/chromedriver

# COMMAND ----------

# MAGIC %sh unzip /tmp/chromedriver_linux64.zip -d /tmp/chromedriver/

# COMMAND ----------

# MAGIC %sh sudo add-apt-repository ppa:canonical-chromium-builds/stage

# COMMAND ----------

# MAGIC %sh /usr/bin/yes | sudo apt update

# COMMAND ----------

# MAGIC %sh /usr/bin/yes | sudo apt install chromium-browser