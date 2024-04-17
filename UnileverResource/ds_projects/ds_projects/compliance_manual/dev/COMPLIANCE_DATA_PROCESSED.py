# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
from datetime import datetime
import pyspark.sql.functions as F
import pyspark.pandas as ps
from pyspark.sql import Window
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA LOAD

# COMMAND ----------

# MAGIC %md
# MAGIC ## COMPLIANCE 

# COMMAND ----------

link_compliance = "/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/COMPLIANCE/INPUT/COMPLIANCE_PARQUET"

df_compliance = spark.read.parquet(link_compliance)
dict_name = { "Week": "YEARWEEK",
            "Ship to": "DISTRIBUTOR",
            "DP Name": "DPNAME",
            "SNC _ Org Optimized Order": "PROPOSED_QTY",
            "Confirmed Order": "COMFIRMED_QTY",}
for old_name in dict_name:
    df_compliance = df_compliance.withColumnRenamed(old_name, dict_name[old_name])
     

df_compliance = (
    df_compliance.withColumn("DPNAME", F.upper(F.col("DPNAME")))
    .drop("Root cause", "Gap", "__index_level_0__")
)

df_compliance = (
    df_compliance.groupby("DISTRIBUTOR", "DPNAME", "YEARWEEK")
    .agg(
        F.sum(F.col("PROPOSED_QTY")).alias("PROPOSED_QTY"),
        F.sum(F.col("COMFIRMED_QTY")).alias("COMFIRMED_QTY"),
    )
    .withColumn("GAP", F.abs(F.col("COMFIRMED_QTY") - F.col("PROPOSED_QTY")))
    .withColumn("COMPLIANCE_RATE", F.round(1 - F.col("GAP") / F.col("PROPOSED_QTY"), 2))
)

df_compliance = df_compliance.select("DISTRIBUTOR", "DPNAME", "YEARWEEK", "PROPOSED_QTY", "COMPLIANCE_RATE").where("DPNAME NOT LIKE '%NOT ASSIGNED%'")



# COMMAND ----------

display(df_compliance)

# COMMAND ----------

df_compliance = df_compliance.withColumn("EXPLODE_MIN_DATE", F.to_date(F.concat(F.col("YEARWEEK"), F.lit("4")), "yyyywwu")).withColumn("EXPLODE_CURRENT_DATE",  F.to_date(F.concat(F.lit(202402), F.lit("4")), "yyyywwu")).dropna(subset = ["COMPLIANCE_RATE"])

df_compliance_explode_default = df_compliance.groupby("DISTRIBUTOR", "DPNAME").agg(F.min(F.col("EXPLODE_MIN_DATE")).alias("EXPLODE_MIN_DATE"), F.max(F.col("EXPLODE_CURRENT_DATE")).alias("EXPLODE_MAX_DATE"), F.min(F.col("YEARWEEK")).alias("MIN_YW")) \
.withColumn(
        "EXPLODED_DATE",
        F.explode(F.expr("sequence(EXPLODE_MIN_DATE, EXPLODE_MAX_DATE, interval 1 week)")),
    ) \
    .withColumn("YEAR", F.year(F.col("EXPLODED_DATE"))) \
    .withColumn("WEEK_OF_YEAR", F.weekofyear(F.col("EXPLODED_DATE"))) \
    .withColumn("MONTH", F.month(F.col("EXPLODED_DATE"))) \
    .withColumn(
        "YEARWEEK",
        F.concat(F.col("YEAR"), F.lpad(F.col("WEEK_OF_YEAR"), 2, "0")).cast("string")
    ).withColumn(
        "YEARMONTH",
        F.concat(F.col("YEAR"), F.lpad(F.col("MONTH"), 2, "0"))
    )

df_compliance_final_exploded_version = df_compliance.join(df_compliance_explode_default.drop("EXPLODE_MIN_DATE", "EXPLODE_MAX_DATE"), on = ["DPNAME", "DISTRIBUTOR", "YEARWEEK"], how = "right").withColumn("has_er", F.when(F.col("COMPLIANCE_RATE").isNull(), False).otherwise(True)).where(f"YEARWEEK > MIN_YW - 1")

# COMMAND ----------

display(df_compliance_final_exploded_version)

# COMMAND ----------

# MAGIC %md
# MAGIC # EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ## SET THRESHOLDS

# COMMAND ----------

null_threshold = 60
er_yw_threshold = 8

# COMMAND ----------

# MAGIC %md
# MAGIC ## FUNCTION THAT CHECKS FOR THE NUMBER OF MISSING YW PER DP|SHIPTO KEY

# COMMAND ----------

df_compliance_final_exploded_version_special_case_counts = (
    df_compliance_final_exploded_version.groupby("DPNAME", "DISTRIBUTOR")
    .agg(
        F.count(F.when(F.col("COMPLIANCE_RATE").isNull(), F.col("YEARWEEK"))).alias(
            "NULL_YW_COUNT"
        ),
        F.count(F.when(F.col("COMPLIANCE_RATE") == 0, F.col("YEARWEEK"))).alias(
            "0_YW_COUNT"
        ),
        F.count(F.when(F.col("COMPLIANCE_RATE") == 1, F.col("YEARWEEK"))).alias(
            "1_YW_COUNT"
        ),F.count(F.when(F.col("COMPLIANCE_RATE").isNotNull(), F.col("YEARWEEK"))).alias(
            "has_ER_YW_COUNT"
        ),
        F.count(F.col("YEARWEEK")).alias("TOTAL_COUNT"),
    )
    .withColumn(
        "NULL_PERCENT", F.round(F.col("NULL_YW_COUNT") / F.col("TOTAL_COUNT"), 2)
    )
    .withColumn(
        f"NULL_UNDER_{null_threshold}", F.when(F.col("NULL_PERCENT") > null_threshold/100, False).otherwise(True)
    )
)

# COMMAND ----------

display(df_compliance_final_exploded_version_special_case_counts)

# COMMAND ----------

# def pyspark_value_counts(df, target_col):
#   '''
#   add a condition to check that the col is categorical
#   '''
#   distinct_list = df.select(target_col).distinct().rdd.flatMap(lambda x: x).collect()
#   list_distinct_vals = [i for i in distinct_list]
#   data = [[], []]
#   col_names = [target_col, "actual_count" ,"percent", "total_count"]
#   total_count = df.count()
#   for i in range(len(list_distinct_vals)):
#     count = df.where(f"{target_col} == {list_distinct_vals[i]}").count()
#     data[i].append(list_distinct_vals[i])
#     data[i].append(count)
#     data[i].append(round(count/total_count, 2))
#     data[i].append(total_count)
#   df_output = spark.createDataFrame(data, col_names)
#   return df_output
    
  

# COMMAND ----------

display(pyspark_value_counts(df_compliance_final_exploded_version_special_case_counts, f"NULL_UNDER_{null_threshold}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter by threshold

# COMMAND ----------

df_compliance_threshold = df_compliance_final_exploded_version_special_case_counts.where(f"NULL_UNDER_{null_threshold} is TRUE AND has_ER_YW_COUNT > {er_yw_threshold}").select("DPNAME", "DISTRIBUTOR")
display(df_compliance_threshold)

# COMMAND ----------

# # create bins
bins = [round(i,2) for i in np.arange(0, 1.1, 0.1)]
print(bins)
# create labels for bins
labels = [f"{round(i,2)} - {round(i+0.1, 2)}" for i in bins[0:len(bins)-1]]
print(labels)
# add a new column with bin labels
df_compliance_final_exploded_version_pd = df_compliance_final_exploded_version.toPandas()
df_compliance_final_exploded_version_pd['truth_bin'] = pd.cut(df_compliance_final_exploded_version_pd['COMPLIANCE_RATE'], bins=bins, labels=labels)
df_compliance_final_exploded_version_pd['truth_bin_adjusted'] = np.where(df_compliance_final_exploded_version_pd['COMPLIANCE_RATE'] == 1, "1", np.where(df_compliance_final_exploded_version_pd['COMPLIANCE_RATE'] == 0, "0", np.where( df_compliance_final_exploded_version_pd['COMPLIANCE_RATE'].isnull(), "null",  df_compliance_final_exploded_version_pd['truth_bin']))) 


# COMMAND ----------

display(df_compliance_final_exploded_version_pd["truth_bin_adjusted"].value_counts(normalize= True))

# COMMAND ----------

df_init = pd.DataFrame()
for ym in df_compliance_final_exploded_version_pd["YEARMONTH"].drop_duplicates():
  df = df_compliance_final_exploded_version_pd[df_compliance_final_exploded_version_pd["YEARMONTH"] == ym]
  df_distribution = df["truth_bin_adjusted"].value_counts(normalize = True).reset_index(name  = "percent")
  df_distribution["YEARMONTH"] = ym 
  df_init = pd.concat([df_init, df_distribution])
  
  print(f"finished yearmonth {ym}")



df = pd.pivot(df_init[~df_init["YEARMONTH"].isin(["202205", "202312", "202401"])], index='index', columns='YEARMONTH', values='percent').reset_index()
display(df)

# COMMAND ----------

display(df_init)

# COMMAND ----------

import matplotlib.pyplot as plt

# df = (
#     df_compliance_final_exploded_version_pd[["YEARMONTH", "truth_bin_adjusted"]]
#     .value_counts(normalize=True)
#     .reset_index(name="percent")
# )

fig, ax = plt.subplots()

x = [i for i in df_init["index"].drop_duplicates()]

for i in df_init[~df_init["YEARMONTH"].isin(["202205", "202312", "202401"])]["YEARMONTH"].drop_duplicates():
    ax.plot(
        x,
        df_init[df_init["YEARMONTH"] == i]["percent"],
        label=i,
    )

# plt.legend()

# Show the plot
plt.show()

# COMMAND ----------

df_compliance_final_exploded_version = df_compliance_final_exploded_version.drop("MONTH", "YEARMONTH")

# COMMAND ----------

# MAGIC %md
# MAGIC # FEATURE ENGINEERING

# COMMAND ----------

# MAGIC %md
# MAGIC ## HOLIDAY AND DATE/WEEK

# COMMAND ----------

df_datetime = pd.read_excel("/dbfs/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/calendar.xlsx", header = 0, sheet_name = "BASEWEEK-CALENDAR")

win_holiday = Window().orderBy("YEARWEEK")

win_quarter = Window().partitionBy("YEARQUARTER").orderBy("YEARQUARTER")

df_datetime = spark.createDataFrame(df_datetime).withColumnRenamed("WEEKORDER", "WEEK_ORDER_MONTH").withColumn("IS_HOLIDAY_WEEK", F.when(F.col("HOLIDAYNAME") == "NORMAL", False).otherwise(True)).withColumn("WAS_LAST_WEEK_HOLIDAY", F.lag(F.col("IS_HOLIDAY_WEEK"), 1).over(win_holiday)).withColumn("IS_NEXT_WEEK_HOLIDAY", F.lead(F.col("IS_HOLIDAY_WEEK"), 1).over(win_holiday)).withColumn("WEEK_ORDER_QUARTER", F.row_number().over(win_quarter)).withColumn("YEAR", F.substring(F.col("YEARWEEK"), 1, 4)).select("YEARWEEK", "YEAR","QUARTER","MONTH", "WEEK_ORDER_QUARTER", "WEEK_ORDER_MONTH", "HOLIDAYNAME", "WEEKTYPE", "IS_HOLIDAY_WEEK", "WAS_LAST_WEEK_HOLIDAY", "IS_NEXT_WEEK_HOLIDAY").dropna()

display(df_datetime)

# COMMAND ----------

display( pd.read_excel("/dbfs/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/calendar.xlsx", header = 0, sheet_name = "BASEWEEK-CALENDAR"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRICE CHANGE / WEIGHT CHANGE

# COMMAND ----------

link_price_change = "/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/price_change_2022_2023.csv"
df_price_change = spark.read.csv(link_price_change, header=True)
dict_name_price =   {
        "Yearweek": "YEARWEEK",
        "DP Name": "DPNAME",
        "Net weight": "WEIGHT_CHANGE",
        "Price change": "PRICE_CHANGE"
    }
for old_name in dict_name_price:
    df_price_change = df_price_change.withColumnRenamed(old_name, dict_name_price[old_name])
df_price_change = df_price_change.withColumn("WEIGHT_CHANGE", F.when(F.col("WEIGHT_CHANGE") == "null", 0).otherwise(1)).withColumn("PRICE_CHANGE", F.when(F.col("PRICE_CHANGE") == "null", 0).otherwise(1))

display(df_price_change)

# COMMAND ----------

display(df_price_change.agg(F.max(F.col("YEARWEEK")), F.min(F.col("YEARWEEK"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRODUCT MASTER

# COMMAND ----------

df_product_master = pd.read_excel("/dbfs/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/product_master_202402.xlsx", header = 0, sheet_name = "PH")[["ULV code", "Pcs/cs", "Category", "DP Name", "Product Classification", "Banded"]]
df_product_master.columns = ["MATERIAL", "PCS/CS", "CATEGORY", "DPNAME", "PRODUCT_CLASSIFICATION", "BANDED"]
df_product_master = spark.createDataFrame(df_product_master).withColumn("DPNAME", F.upper(F.col("DPNAME"))).withColumn("CATEGORY", F.upper(F.col("CATEGORY"))).withColumn("PRODUCT_CLASSIFICATION", F.upper(F.col("PRODUCT_CLASSIFICATION"))).withColumn("BANDED", F.upper(F.col("BANDED")))
display(df_product_master)

# COMMAND ----------

display(df_product_master)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CUSTOMER MASTER

# COMMAND ----------

df_cust_master = spark.read.parquet("/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/customer_master.parquet").select("DISTRIBUTOR", "BANNER")

# COMMAND ----------

display(df_cust_master)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PROMOTION

# COMMAND ----------

df_promotion = spark.read.parquet("/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/DT_PROMOTION_FEATURES_DP_BANNER_YW.parquet").where("DPNAME is NOT NULL").drop("FIRST_WEEK", "LAST_WEEK", "MID_WEEK", "HOLIDAY_WEEK", "NON_HOLIDAY_WEEK")
display(df_promotion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MANUAL

# COMMAND ----------

link_manual = "/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/MANUAL_PARQUET" # current manual 
df_manual = spark.read.parquet(link_manual)
df_manual = (
    df_manual.withColumnRenamed("Yearweek", "YEARWEEK").withColumnRenamed(
            "Ship-to pa", "DISTRIBUTOR").withColumnRenamed(
            "DP Name", "DPNAME").withColumnRenamed(
            "Order type", "ORDER_TYPE").withColumnRenamed(
            "Order Qty", "ORDER_QUANTITY").withColumn("DPNAME", F.upper(F.col("DPNAME")))
    .drop("Last Cust", "Last Cust value", "Order Qty value")
)

df_manual = (
    df_manual.groupby("DISTRIBUTOR", "DPNAME", "YEARWEEK")
    .pivot("ORDER_TYPE")
    .sum("ORDER_QUANTITY")
    .fillna(0)
    .withColumn(
        "TOTAL_ORDER_QTY",
        F.col("MANUAL ALLOCATION") + F.col("MANUAL ORDER") + F.col("SNC"),
    )
    .withColumn(
        "MANUAL_RATE", F.round(F.col("MANUAL ORDER") / F.col("TOTAL_ORDER_QTY"), 2)
    ).where("TOTAL_ORDER_QTY > 0 OR DPNAME == 'NOT ASSIGNED'").fillna(0).select("DPNAME", "DISTRIBUTOR", "YEARWEEK", "MANUAL_RATE", "TOTAL_ORDER_QTY")
)
display(df_manual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PROD CLASS BY DPNAME

# COMMAND ----------

"""
LOGIC: sum sales to dpname, reorder in desc, top 80% is A, the next 15% is B, the rest is C
"""

df_sec_sales = (
    spark.read.parquet(
        "/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/LE_SEC_SALES_YAFSU_FORMAT_PARQUET"
    )
    .groupby("DPNAME")
    .agg(F.sum(F.col("CS_WEEK")).alias("PS"))
)

win_sum = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
win_cumsum = Window.orderBy(F.desc("proportion")).rowsBetween(Window.unboundedPreceding , 0)

df_class = df_sec_sales.withColumn("sum", F.sum(F.col("PS")).over(win_sum)).withColumn("proportion", F.round(F.col("PS")/F.col("sum"), 4)).withColumn("cum_sum_percent", F.sum(F.col("proportion")).over(win_cumsum)).withColumn("CLASS", F.when(F.col("cum_sum_percent") <= 0.8, "A").otherwise(F.when(F.col("cum_sum_percent") <= 0.95, "B").otherwise("C")))


display(df_class.orderBy("proportion", ascending = False))

# COMMAND ----------

# MAGIC %md
# MAGIC # JOIN ALL THE FEATURES TO THE MAIN DATA

# COMMAND ----------

df_input = (
    df_compliance_final_exploded_version.drop("EXPLODED_DATE", "YEAR", "WEEK_OF_YEAR")
    .fillna(0, subset=["COMPLIANCE_RATE"])
    .join(
        df_product_master.select("DPNAME", "CATEGORY").distinct(),
        on="DPNAME",
        how="left",
    )
    .join(df_cust_master, on="DISTRIBUTOR", how="left")
    .join(df_datetime, on="YEARWEEK", how="left")
    .join(df_price_change, on=["DPNAME", "YEARWEEK"], how="left")
    .join(df_manual, on=["DPNAME", "DISTRIBUTOR", "YEARWEEK"], how="left")
    .join(df_promotion, on=["DPNAME", "BANNER", "YEARWEEK"], how="left").join(df_class.select("DPNAME", "CLASS"), on = "DPNAME", how = "left" )
    .fillna(0)
    .drop(
        "EXPLODE_MIN_DATE",
        "EXPLODE_CURRENT_DATE",
        "MIN_YW",
        "HOLIDAYNAME",
        "TOTAL_ORDER_QTY",
    )
)


# COMMAND ----------

display(df_input_over_60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## HISTORICAL FEATURES

# COMMAND ----------

def lag_and_lag_rolling_generator(df, var_name):
  '''
  Function to generate laf features: min, max, mean, median for historical features, such as compliance, manual, number of ones and zeros

PARAMS: 
- df: a dataframe that contains the target var
- var_name: name of the target var

OUTPUT: a new dataframe

  '''
  lags = [2, 3, 4, 5, 8, 9, 12, 13, 16]
  lag_rolling = [(2, 4), (4, 4), (8, 4), (12, 4), (2, 8), (4, 8), (8, 8), (12, 8)]
  win_lag = Window().partitionBy("DPNAME", "DISTRIBUTOR").orderBy("YEARWEEK")
  for i in lags:
    df = df.withColumn(
        f"{var_name}_lag_{i}", F.lag(F.col(var_name), i).over(win_lag)
    )
  for rolling_tuple in lag_rolling:
    n = rolling_tuple[1] - 1
    win_rolling = (
        Window.partitionBy("DPNAME", "DISTRIBUTOR")
        .orderBy("YEARWEEK")
        .rowsBetween(-n, 0)
    )
    df = df.withColumn(
        f"{var_name}_lag_{rolling_tuple[0]}_rolling_mean_{rolling_tuple[1]}",
        F.avg(f"{var_name}_lag_{rolling_tuple[0]}").over(win_rolling),
    ).withColumn(
    f"{var_name}_lag_{rolling_tuple[0]}_rolling_median_{rolling_tuple[1]}",
    F.expr(f"percentile_approx({var_name}_lag_{rolling_tuple[0]}, 0.5)").over(win_rolling)
).withColumn(
        f"{var_name}_lag_{rolling_tuple[0]}_rolling_max_{rolling_tuple[1]}",
        F.max(f"{var_name}_lag_{rolling_tuple[0]}").over(win_rolling),
    ).withColumn(
        f"{var_name}_lag_{rolling_tuple[0]}_rolling_min_{rolling_tuple[1]}",
        F.min(f"{var_name}_lag_{rolling_tuple[0]}").over(win_rolling),
    )
  return df

# COMMAND ----------

l_lag_features = ["COMPLIANCE_RATE", "MANUAL_RATE", "PROPOSED_QTY" ]
for i in l_lag_features:
  df_input = lag_and_lag_rolling_generator(df_input, i)

# COMMAND ----------

# display(df_input)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZEROS AND ONES OVER ROLLING WINDOWS

# COMMAND ----------

lag_rolling = [(2, 4), (4, 4), (8, 4), (12, 4), (2, 8), (4, 8), (8, 8), (12, 8)]
for val in [0, 1]:
  for rolling_tuple in lag_rolling:
      n = rolling_tuple[1] - 1
      win_rolling = (
          Window.partitionBy("DPNAME", "DISTRIBUTOR")
          .orderBy("YEARWEEK")
          .rowsBetween(-n, 0)
      )
      df_input = df_input.withColumn(
          f"{val}_compliance_count_lag_{rolling_tuple[0]}_rolling_{rolling_tuple[1]}",F.sum(F.when(F.col(f"COMPLIANCE_RATE_lag_{rolling_tuple[0]}") == val, 1).otherwise(0)).over(win_rolling)
          )
      

# COMMAND ----------

# MAGIC %md
# MAGIC # FINALIZE DATA

# COMMAND ----------

df_input_under_60 = df_input.join(df_compliance_threshold, on=["DPNAME", "DISTRIBUTOR"], how="inner")

df_input_over_60 = df_input.join(df_compliance_threshold, on=["DPNAME", "DISTRIBUTOR"], how="leftanti")

full_key_count = df_input.select("DPNAME", "DISTRIBUTOR").distinct().count()
un60_key_count = df_input_under_60.select("DPNAME", "DISTRIBUTOR").distinct().count()
ov60_key_count = df_input_over_60.select("DPNAME", "DISTRIBUTOR").distinct().count()
print(f"full key count is: {full_key_count}, under 60 count is: {un60_key_count} whereas over 60 count is: {ov60_key_count}")

# COMMAND ----------

import datetime 

current_date = datetime.date.today().strftime("%Y%m%d")

df_input_under_60.repartition(4).write.mode("overwrite").parquet(
    f"dbfs:/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/COMPLIANCE/INPUT/COMPLIANCE_ENGINEERED_quality_PARQUET")

df_input_over_60.repartition(4).write.mode("overwrite").parquet(
    f"dbfs:/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS//COMPLIANCE/INPUT/COMPLIANCE_ENGINEERED_questionable_PARQUET")
