# Databricks notebook source
# DBTITLE 1,Setup Environment
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

# DBTITLE 1,Setup YAFSU project
# MAGIC %run "./YAFSU_Landing_Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC # Import related ML Libraries + Utils

# COMMAND ----------

# MAGIC %run "./YAFSU_Utils"

# COMMAND ----------

# MAGIC %run "./YAFSU_AutoForecast_Library"

# COMMAND ----------

# MAGIC %md
# MAGIC # API Interface Run the Models

# COMMAND ----------

df_input_validated = pd.read_parquet(TEMP_PATH + "DF_VALIDATED.parquet")
YAFSU_AutoForecast_main_run(df_validated= df_input_validated)