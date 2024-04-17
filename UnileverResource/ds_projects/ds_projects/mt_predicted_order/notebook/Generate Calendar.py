# Databricks notebook source
# MAGIC %pip install --upgrade holidays
# MAGIC %pip install --upgrade lunardate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate a range of dates (e.g., for the year 2023)
# MAGIC WITH date_range AS (
# MAGIC   SELECT explode(sequence(to_date('2015-01-01'), to_date('2030-01-01'), interval 1 day)) AS date
# MAGIC )
# MAGIC
# MAGIC -- Select date components from the generated range
# MAGIC SELECT
# MAGIC   d.date,
# MAGIC   year(d.date) AS year,
# MAGIC   quarter(d.date) AS quarter,
# MAGIC   month(d.date) AS month,
# MAGIC   day(d.date) AS dayofmonth,
# MAGIC   weekofyear(d.date) AS weekofyear,
# MAGIC   CASE
# MAGIC     WHEN month(date) = 1 AND weekofyear(date) > 51 THEN concat(year(d.date) - 1, 52)
# MAGIC     ELSE concat(year(d.date), lpad(weekofyear(d.date), 2, '0'))
# MAGIC   END AS yearweek,
# MAGIC   dayofweek(d.date) AS dayofweek,
# MAGIC   dayofyear(d.date) AS day_of_year,
# MAGIC   CASE WHEN dayofweek(d.date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS weekend,
# MAGIC
# MAGIC   CASE 
# MAGIC     WHEN month(d.date) IN (3, 4, 5) THEN 'Spring'
# MAGIC     WHEN month(d.date) IN (6, 7, 8) THEN 'Summer'
# MAGIC     WHEN month(d.date) IN (9, 10, 11) THEN 'Autumn'
# MAGIC     ELSE 'Winter'
# MAGIC   END AS season
# MAGIC
# MAGIC FROM date_range d

# COMMAND ----------

df_spark = _sqldf.pandas_api()

# COMMAND ----------

import holidays
from lunardate import LunarDate
import pyspark.sql.functions as F
import pyspark.pandas as ps

# Add datetime
df_spark["date"] = ps.to_datetime(df_spark["date"])
df_spark["year"] = df_spark["date"].dt.year
df_spark["quarter"] = df_spark["date"].dt.quarter
df_spark["month"] = df_spark["date"].dt.month
df_spark["dayofweek"] = df_spark["date"].dt.dayofweek
df_spark["weekend"] = df_spark["dayofweek"].apply(lambda x: True if x in [0, 6] else False)
df_spark["dayofyear"] = df_spark["date"].dt.dayofyear
df_spark["weekofyear"] = df_spark["date"].dt.weekofyear



df_spark["is_year_start"] = df_spark["date"].dt.is_year_start
df_spark["is_year_end"] = df_spark["date"].dt.is_year_end

df_spark["is_quarter_start"] = df_spark["date"].dt.is_quarter_start
df_spark["is_quarter_end"] = df_spark["date"].dt.is_quarter_end

df_spark["is_month_start"] = df_spark["date"].dt.is_month_start
df_spark["is_month_end"] = df_spark["date"].dt.is_month_end

df_spark["is_leap_year"] = df_spark["date"].dt.is_leap_year




# # Add holidays
vn_holidays = holidays.country_holidays('VN')
us_holidays = holidays.country_holidays('US')
df_spark["vn_holiday"] = df_spark["date"].apply(lambda x: vn_holidays.get(x) if vn_holidays.get(x) is not None else "Non-Holiday")
df_spark["us_holiday"] = df_spark["date"].apply(lambda x: us_holidays.get(x) if us_holidays.get(x) is not None else "Non-Holiday")

# # Add Lunar Date
df_spark["lunar_date"] = ps.to_datetime(df_spark["date"].apply(lambda x: LunarDate.fromSolarDate(x.year, x.month, x.day).toSolarDate()))
df_spark["lunar_year"] = df_spark["lunar_date"].apply(lambda x: x.year)
df_spark["lunar_month"] = df_spark["lunar_date"].apply(lambda x: x.month)
df_spark["lunar_day"] = df_spark["lunar_date"].apply(lambda x: x.day)



# COMMAND ----------

df_spark.to_spark().cache().createOrReplaceTempView("mt_calendar_master")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC date,
# MAGIC year,
# MAGIC quarter,
# MAGIC month,
# MAGIC dayofmonth,
# MAGIC dayofweek,
# MAGIC weekend,
# MAGIC dayofyear,
# MAGIC weekofyear,
# MAGIC yearweek,
# MAGIC
# MAGIC is_year_start,
# MAGIC is_year_end,
# MAGIC is_quarter_start,
# MAGIC is_quarter_end,
# MAGIC is_month_start,
# MAGIC is_month_end,
# MAGIC
# MAGIC is_leap_year,
# MAGIC vn_holiday,
# MAGIC us_holiday,
# MAGIC
# MAGIC lunar_date,
# MAGIC lunar_year,
# MAGIC lunar_month,
# MAGIC lunar_day,
# MAGIC season
# MAGIC
# MAGIC from mt_calendar_master

# COMMAND ----------

_sqldf.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("mt_predicted_order.mt_calendar_master")

# COMMAND ----------

import pyspark.pandas as ps
import pandas as pd

df = pd.read_excel("/dbfs/mnt/adls/SGC_BIGC_DS/MASTER/List Core SKUs.xlsx")

df_spark = spark.createDataFrame(df)
df_spark = df_spark.withColumnRenamed("Banner", "banner")
df_spark = df_spark.withColumnRenamed("DP Name", "dp_name")

(
    df_spark
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("mt_predicted_order.core_skus")

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from mt_predicted_order.core_skus

# COMMAND ----------

