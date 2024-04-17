# Databricks notebook source
dbutils.widgets.text("BANNER_REMOVE_REPEATED", "")
dbutils.widgets.text("BANNER_PROMOTION", "")
dbutils.widgets.text("BANNER_NAME", "")
dbutils.widgets.text("USER_TRIGGER", "")

# COMMAND ----------

BANNER_REMOVE_REPEATED = dbutils.widgets.get("BANNER_REMOVE_REPEATED")
BANNER_PROMOTION = dbutils.widgets.get("BANNER_PROMOTION")
BANNER_NAME = dbutils.widgets.get("BANNER_NAME")
USER_TRIGGER = dbutils.widgets.get("USER_TRIGGER")

for check_item in [BANNER_REMOVE_REPEATED, BANNER_PROMOTION, BANNER_NAME, USER_TRIGGER]:
  if check_item == "":
    raise Exception("STOP PROGRAM - NO DATA TO RUN")

# BANNER_REMOVE_REPEATED = "2022_AEON REMOVE REPEATED FILE.parquet.gzip_Lai-Trung-Minh Duc.PDF"
# BANNER_PROMOTION = "2022_PROMOTION TRANSFORMATION_UPDATEDAEON.par_Lai-Trung-Minh Duc.PDF"
# BANNER_NAME = "AEON"
# USER_TRIGGER = "Lai-Trung-Minh.Duc@unilever.com"

# COMMAND ----------

dbutils.notebook.run("./01.MT_DEMAND_POST_CREATE", timeout_seconds=10000, 
                     arguments = {
  "BANNER_REMOVE_REPEATED": BANNER_REMOVE_REPEATED,
  "BANNER_PROMOTION": BANNER_PROMOTION,
  "BANNER_NAME": BANNER_NAME,
  "USER_TRIGGER": USER_TRIGGER
})

# COMMAND ----------

dbutils.notebook.run("./02.MODEL_VOLUME_CSE", timeout_seconds=10000, arguments = {
  "BANNER_REMOVE_REPEATED": BANNER_REMOVE_REPEATED,
  "BANNER_PROMOTION": BANNER_PROMOTION,
  "BANNER_NAME": BANNER_NAME,
  "USER_TRIGGER": USER_TRIGGER
})

# COMMAND ----------

dbutils.notebook.run("./03.EXPORT_TO_EXCEL", timeout_seconds=10000, arguments = {
  "BANNER_REMOVE_REPEATED": BANNER_REMOVE_REPEATED,
  "BANNER_PROMOTION": BANNER_PROMOTION,
  "BANNER_NAME": BANNER_NAME,
  "USER_TRIGGER": USER_TRIGGER
})