# Databricks notebook source
import os
import pandas as pd
import pyspark.sql.functions as F
import datetime as d

# COMMAND ----------

# MAGIC %md
# MAGIC # TRUTH TABLE

# COMMAND ----------

link_input = "/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/LE_SEC_SALES_YAFSU_FORMAT_PARQUET" 
df_input = spark.read.parquet(link_input)
df_input_agg = df_input.groupby("DISTRIBUTOR", "DPNAME", "YEARWEEK").agg(
    F.sum(F.col("CS_WEEK")).alias("CS_WEEK")
).withColumn("KEY_TS", F.concat(F.col("DISTRIBUTOR"), F.lit("|"), F.col("DPNAME"))).drop("DISTRIBUTOR", "DPNAME")
display(df_input_agg)

# COMMAND ----------

# display(df_input_non_banded.where("DPNAME == 'POND PINKISH SL CREAM 50G' AND DISTRIBUTOR == 15539395 AND YEARWEEK == 202342 "))

# COMMAND ----------

# MAGIC %md
# MAGIC # POST PROCESS 

# COMMAND ----------

from datetime import date
import os
run_on = date.today().strftime("%Y%m%d")
run_year = date.today().isocalendar().year
run_week_order = date.today().isocalendar().week
run_yearweek = run_year*100 + run_week_order - 2
# run_yearweek = 202401
yw_cutoff_range = [*range(run_yearweek, run_yearweek+1)]

df_input_test = (
    df_input_agg
    .withColumnRenamed("CS_WEEK", "CS_WEEK_ACTUAL")
    .withColumnRenamed("YEARWEEK", "YW_FC")
    .select("YW_FC", "KEY_TS", "CS_WEEK_ACTUAL")
)

dayOfWeekRange = [
    # "CS_Friday",
    # "CS_Monday",
    # "CS_Saturday",
    # "CS_Thursday",
    # "CS_Wednesday",
    # "CS_Tuesday",
    "CS_WEEK",
    # "HALF_WEEK_1"
]

for dayOfWeek in dayOfWeekRange:
    for YWcutOff in yw_cutoff_range:
        link_download = f"/mnt/adls/DT_PREDICTEDORDER/OUTPUTS/TOTAL/{dayOfWeek}/{run_on}_LE_DTSS_TOTAL_NO_PROMO_CS_WEEK_CUTOFF_{YWcutOff}.parquet"
        target_name = "NIXTLA_MLFC_" + dayOfWeek + "_MEDIAN"
        df_sample = spark.read.parquet(link_download)
        yw_fc_lag2 = YWcutOff + 2
        yw_fc_lag3 = YWcutOff + 3
        df_sample = df_sample.filter((F.col("YEARWEEK") == yw_fc_lag2 ) | (F.col("YEARWEEK") == yw_fc_lag3 ))
        og_count = df_sample.select("KEY_TS").distinct().count()
        df_sample = (
            df_sample.withColumnRenamed("YEARWEEK", "YW_FC")
            .withColumn("split_col", F.split(df_sample["KEY_TS"], "\\|"))
            .withColumn("Ship to", F.col("split_col")[0])
            .withColumn("DP Name", F.col("split_col")[1])
            .withColumn("YW_CUTOFF", F.lit(YWcutOff))
            .withColumnRenamed(target_name, "FC (cs)")
            .withColumn("GAP_CUTOFF", - F.col("YW_CUTOFF") + F.col("YW_FC") - 2)
            .withColumn(
                "CORRECT_LAG_2",
                F.when( (F.col("YW_FC") == yw_fc_lag2 ) & (F.col("LAG_FC") ==  2), True).otherwise(
                    False
                ),
            ).withColumn(
                "CORRECT_LAG_3",
                F.when((F.col("YW_FC") == yw_fc_lag3 ) & (F.col("LAG_FC") ==  3), True).otherwise(
                    False
                )
        ).filter(F.col("Ship to") != 0)
        )

        df_sample = df_sample.select(
            F.col("YW_FC"),
            F.col("YW_CUTOFF"),
            F.col("GAP_CUTOFF"),
            F.col("Ship to"),
            F.col("DP Name"),
            F.col("FC (cs)"),
            F.col("KEY_TS"),
            F.col("CORRECT_LAG_2"),
            F.col("CORRECT_LAG_3"),
             F.col("LAG_FC")
        )
  
        print(f"This is for {dayOfWeek}_{YWcutOff}: unique mapping count is {og_count}")

        if YWcutOff == 202335:
            df_full = df_sample.join(
                df_input_test, on=["KEY_TS", "YW_FC"], how="left"
            )

        else:
            df_merge_test = df_sample.join(
                df_input_test, on=["KEY_TS", "YW_FC"], how="left"
            )
            # df_full = df_full.union(df_merge_test)

df_merge_test = df_merge_test.fillna(0)
df_full_pd = df_merge_test.toPandas()
df_full_pd.to_csv(f"/dbfs/mnt/adls/DT_PREDICTEDORDER/POST_PROCESSED_OUTPUTS/TOTAL/{dayOfWeek}/{dayOfWeek}_YAFSU_DTSS_PREDS_NO_PROMO_TOTAL_CUTOFF_{YWcutOff}_FC_{YWcutOff+2}.csv")

display(df_full_pd)

# COMMAND ----------

