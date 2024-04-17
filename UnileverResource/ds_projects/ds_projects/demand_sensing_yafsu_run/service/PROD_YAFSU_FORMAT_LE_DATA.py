# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from pyspark.sql import  Window
NUMBER_OF_PARTITIONS = sc.defaultParallelism * 2

# COMMAND ----------

import datetime as dt

year = dt.date.today().isocalendar().year
week = dt.date.today().isocalendar().week
current_yw = year*100 + week
print(current_yw)

# COMMAND ----------

# MAGIC %md
# MAGIC # READ RAW LE DATA

# COMMAND ----------

link_data = (
    "/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/LE_SEC_SALES_RAW_UPDATED_PARQUET"
)
df_sec_sales_raw = spark.read.parquet(link_data)
df_sec_sales_raw = (
    df_sec_sales_raw.withColumn("BANNER", F.concat(F.lit("DT "), F.upper(F.col("BANNER"))))
    .withColumn(
        "BANNER",
        F.when(F.col("BANNER") == "DT HO CHI MINH - EAST", F.lit("DT HCME")).otherwise(
            F.col("BANNER")
        ),
    )
    .where(
        "OUTLET_TYPES not like '%ATWORK%' AND OUTLET_TYPES not like '%PRO%' AND OUTLET_TYPES not like '%INTERNAL%' "
    )

)
# remove at-work, internal and pro

# COMMAND ----------

display(df_sec_sales_raw.agg(F.max("YEARWEEK")))

# COMMAND ----------

# MAGIC %md
# MAGIC # PRODUCT MASTER

# COMMAND ----------

'''
Temporary solution. NEED to find a way to encode the direct file from sharepoint, right now the problem is that the og file is hard to read on databricks
'''

df_product_master = pd.read_excel("/dbfs/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALES_DEPENDENT_FILES/product_master.xlsx", header = 0, sheet_name = "PH")[["ULV code", "Pcs/cs", "Category", "DP Name", "Product Classification", "Banded"]]
df_product_master.columns = ["MATERIAL", "PCS/CS", "CATEGORY", "DPNAME", "PRODUCT_CLASSIFICATION", "BANDED"]
df_product_master = spark.createDataFrame(df_product_master).withColumn("DPNAME", F.upper(F.col("DPNAME"))).withColumn("CATEGORY", F.upper(F.col("CATEGORY"))).withColumn("PRODUCT_CLASSIFICATION", F.upper(F.col("PRODUCT_CLASSIFICATION"))).withColumn("BANDED", F.upper(F.col("BANDED")))
display(df_product_master)

# COMMAND ----------

# MAGIC %md
# MAGIC # EXPECTED OUTPUT

# COMMAND ----------

# df_expected_dpname = spark.read.csv("/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALES_DEPENDENT_FILES/expected_dpname_202402.csv", header = True).withColumnRenamed("DP NAME", "DPNAME").withColumn("DPNAME", F.trim(F.upper(F.col("DPNAME"))))
# for col_name in df_expected_dpname.columns[1:]:
#   df_expected_dpname = df_expected_dpname.withColumn(col_name, F.when(F.col(col_name).isNull(), "null").otherwise(col_name))
# df_expected_dpname = df_expected_dpname.withColumn("sold_in", F.concat(F.col("DT CENTRAL"),F.lit(","), F.col("DT HCME"),F.lit(","), F.col("DT MEKONG DELTA"),F.lit(","), F.col("DT NORTH") )).withColumn("banner_array", F.split(F.col("sold_in"), ",")).withColumn("BANNER", F.explode(F.col("banner_array"))).where("BANNER != 'null'").select("DPNAME", "BANNER")
# display(df_expected_dpname.groupby("DPNAME").count())


# COMMAND ----------

# df_expected_distributor = spark.read.csv("/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALES_DEPENDENT_FILES/expected_dist_202402.csv", header = True).withColumnRenamed("Region","BANNER").withColumnRenamed("Ship to","DISTRIBUTOR").withColumn("BANNER", F.upper(F.col("BANNER")))
# display(df_expected_distributor.select("BANNER").distinct())

# COMMAND ----------

# df_expected_output = df_expected_distributor.join(df_expected_dpname, on = "BANNER", how = "left").withColumn("MAPPING", F.concat(F.col("DISTRIBUTOR"), F.lit("|"), F.col("DPNAME"))).withColumn("EXPECTED", F.lit(True))

# display(df_expected_output.count())

# COMMAND ----------

df_expected_output = spark.createDataFrame(pd.read_excel("/dbfs/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALES_DEPENDENT_FILES/expected_mappings_202407.xlsx", header = 0, sheet_name = "Sheet1"))

df_expected_output = df_expected_output.withColumnRenamed("Ship to", "DISTRIBUTOR").withColumnRenamed("Skus","MATERIAL").join(df_product_master.select("MATERIAL", "DPNAME"), on = "MATERIAL", how = "left").drop("MATERIAL").withColumn("MAPPING", F.concat(F.col("DISTRIBUTOR"), F.lit("|"), F.col("DPNAME"))).withColumn("EXPECTED", F.lit(True))

display(df_expected_output)


# COMMAND ----------

# display(df_expected_output)

# COMMAND ----------

# list_worm_dpname = ["CF CONC. WHITE 20ML", "OMO GOLD 2700 GR", "LIFEBUOY SHAMPOO THICK & SHINY 6G", "SUNLIGHT DWL NATURE ESSENCE 750G", "LIFEBUOYSOAP TOTAL125G" ]

# list_expected_dpname = df_expected_output.select("DPNAME").distinct().rdd.flatMap(lambda x: x).collect()

# for i in list_worm_dpname:
#   if i not in list_expected_dpname:
#     raise Exception(f"{i} is wrong choice")

# COMMAND ----------

# MAGIC %md
# MAGIC #ONLY TAKE EXPECTED MAPPINGS AND JOIN WITH PRODUCT MASTER

# COMMAND ----------

#map to product master
df_sec_sales = df_sec_sales_raw.join(df_product_master, on = "MATERIAL", how = "left").withColumn("MAPPING", F.concat(F.col("DISTRIBUTOR"), F.lit("|"), F.col("DPNAME")))


#take only expected mappings

df_sec_sales = df_sec_sales.join(
    df_expected_output.select("MAPPING", "EXPECTED"), on="MAPPING", how="full"
)
df_sec_sales = df_sec_sales.withColumn(
    "STATUS",
    F.when(
        (F.col("EXPECTED") == True) & (F.col("DPNAME").isNotNull()), "NO ERROR"
    ).otherwise(
        F.when(
            (F.col("EXPECTED") == True) & (F.col("DPNAME").isNull()), "NO LE RECORD"
        ).otherwise("NOT_EXPECTED")
    ),
)

df_sec_sales = df_sec_sales.where("STATUS == 'NO ERROR'")





# COMMAND ----------

display(df_sec_sales.select("BANNER").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC # YAFSU CONDITION CHECK
# MAGIC

# COMMAND ----------

def data_check(df_input):
    from datetime import date
    import os
    run_year = date.today().isocalendar().year
    run_week_order = date.today().isocalendar().week
    run_yearweek = run_year*100 + run_week_order

    # Filter rows based on the condition
    yw_cutoff_input_data = df_input.withColumn(
        "YEARWEEK", F.col("YEARWEEK").cast("int")
    )
    yw_cutoff_input_data = yw_cutoff_input_data.filter(F.col("YEARWEEK") <= run_yearweek)

    # Aggregate data
    yw_cutoff_input_data = (
        yw_cutoff_input_data.groupBy("DPNAME",
                                      "DISTRIBUTOR",
    )
        .agg(
            F.countDistinct("YEARWEEK").alias("YW_COUNT"),
            F.max("YEARWEEK").alias("LAST_SALES_W"),
            F.min("YEARWEEK").alias("EARLIEST_SALES_W"),
            F.sum("PCS").alias("TOTAL_SALES"),
            F.mean("PCS").alias("AVG_SALES"),
            F.max("PCS").alias("MAX_SALES"),
            F.min("PCS").alias("MIN_SALES"),
        )
        .withColumn(
            "CUTOFF_GAP",
            (
                F.lit(run_yearweek)
                - F.col("LAST_SALES_W")
                - 48
                * (
                    run_year
                    - F.substring(F.col("LAST_SALES_W").cast("string"), 1, 4).cast(
                        "int"
                    )
                )
            ),
        ).withColumn(
            "CUTOFF_GAP_FROM_EARLIEST_SALES_WEEK",
            (
                F.lit(run_yearweek)
                - F.col("EARLIEST_SALES_W")
                - 48
                * (
                    run_year
                    - F.substring(F.col("EARLIEST_SALES_W").cast("string"), 1, 4).cast(
                        "int"
                    )
                )
            ),
        )
    )

    # Create a new column "MAPPING"
    yw_cutoff_input_data = yw_cutoff_input_data.withColumn(
        "MAPPING", F.concat("DISTRIBUTOR", F.lit("|"), "DPNAME")
    )

    # Create new columns based on conditions
    yw_cutoff_input_data = yw_cutoff_input_data.withColumn(
        "SALES_WEEK_NUM_MORE_THAN_26",
        F.when(F.col("YW_COUNT") < 26, "FALSE").otherwise("TRUE"),
    )
    yw_cutoff_input_data = yw_cutoff_input_data.withColumn(
        "SALES_WEEK_NUM_MORE_THAN_52",
        F.when(F.col("YW_COUNT") < 52, "FALSE").otherwise("TRUE"),
    )
    yw_cutoff_input_data = yw_cutoff_input_data.withColumn(
        "LAST_SALES_WEEK_WITHIN_26",
        F.when(F.col("CUTOFF_GAP") <= 26, "TRUE").otherwise("FALSE"),
    )

    # Create a new column "WILL_YAFSU_PROVIDE_FC?"
    yw_cutoff_input_data = yw_cutoff_input_data.withColumn(
        "WILL_YAFSU_PROVIDE_FC?",
        F.when(
            (F.col("SALES_WEEK_NUM_MORE_THAN_26") == "FALSE")
            | (F.col("LAST_SALES_WEEK_WITHIN_26") == "FALSE"),
            "ABSOLUTELY_NOT",
        ).otherwise(
            F.when(
                (F.col("SALES_WEEK_NUM_MORE_THAN_26") == "TRUE")
                & (F.col("LAST_SALES_WEEK_WITHIN_26") == "TRUE")
                & (F.col("SALES_WEEK_NUM_MORE_THAN_52") == "FALSE"),
                "MAYBE",
            ).otherwise(
                F.when(
                    (F.col("LAST_SALES_WEEK_WITHIN_26") == "TRUE")
                    & (F.col("SALES_WEEK_NUM_MORE_THAN_52") == "TRUE"),
                    "ABSOLUTELY",
                ).otherwise("NEED_FURTHER_INVESTIGATION")
            )
        ),
    ).withColumn(
    "MAPPING_AGE",
    F.when(F.col("CUTOFF_GAP_FROM_EARLIEST_SALES_WEEK") < 26, "NEW")
    .otherwise(
        F.when(F.col("LAST_SALES_WEEK_WITHIN_26") == 'FALSE', "INACTIVE")
        .otherwise(
            F.when(
                (F.col("LAST_SALES_WEEK_WITHIN_26") == "TRUE") & (F.col("SALES_WEEK_NUM_MORE_THAN_52") == "FALSE"),
                "OLD BUT LACKING SALES"
            ).otherwise("OLD AND ACTIVE")
        )
    )
)


#need to add a way to exclude discontinuous sales 


    # Collect the results to pandas DataFrame
    result_pd_df = yw_cutoff_input_data.toPandas()

    

    # Return the results
    return result_pd_df
 

# COMMAND ----------

df_test = data_check(df_sec_sales)

# COMMAND ----------

# display(df_test)

# COMMAND ----------

display(df_test["WILL_YAFSU_PROVIDE_FC?"].value_counts(normalize = True))

# COMMAND ----------

# MAGIC %md
# MAGIC # YAFSU TRANSFORM DATA

# COMMAND ----------

#Rename banners 
df_sec_sales = df_sec_sales.withColumn("BANNER", F.when(F.col("BANNER") == 'DT HO CHI MINH - EAST', F.lit("DT HCME")).otherwise(F.col("BANNER")))

# Calculate CS column
df_sec_sales = df_sec_sales.withColumn("CS", F.col("PCS") / F.col("Pcs/cs")).withColumnRenamed("UpdatedInvoiceDate","DATE")

# Group by and sum CS column
grouped_df = (
    df_sec_sales.groupBy(
        "BANNER", "DISTRIBUTOR", "DATE", "YEARWEEK", "CATEGORY", "DPNAME", "MATERIAL"
    )
    .agg(F.sum("CS").alias("CS"))
    .withColumn("DATE", F.to_date(F.col("DATE").cast("string"), "yyyy-MM-dd"))
)
# # Extract weekday from DATE
grouped_df = grouped_df.withColumn("WEEKDAY", F.concat(F.lit("CS_"), F.upper(F.date_format("DATE", "EEEE"))))
# 
# display(grouped_df)

# Pivot the table
df_input_pivot = (
    grouped_df.groupby(
        "BANNER", "DISTRIBUTOR", "CATEGORY", "DPNAME", "YEARWEEK", "MATERIAL"
    ) \
    .pivot("WEEKDAY") \
    .sum("CS") \
    .fillna(0)
)

  # # Calculate CS_Week
weekday_columns = ["CS_Friday", "CS_Monday", "CS_Saturday", "CS_Thursday", "CS_Tuesday", "CS_Wednesday"]
df_input_pivot = df_input_pivot.withColumn("CS_WEEK", sum(df_input_pivot[col.upper()] for col in weekday_columns))

  # # Calculate lagged values
window_spec = Window().partitionBy("BANNER", "DISTRIBUTOR", "CATEGORY", "DPNAME", "MATERIAL").orderBy("YEARWEEK")

df_input_pivot = df_input_pivot.withColumn("CS_saturday_lag1", F.lag("CS_Saturday",1).over(window_spec))
df_input_pivot = df_input_pivot.withColumn("CS_sunday_lag1", F.lag("CS_Sunday", 1).over(window_spec)).fillna(0)

  # # Calculate CS_HW1 and CS_HW2
HW1 = ["CS_Monday", "CS_Saturday_lag1", "CS_Sunday_lag1", "CS_Tuesday"]
HW2 = ["CS_Thursday", "CS_Friday", "CS_Wednesday"]
df_input_pivot = df_input_pivot.withColumn(
    "CS_HW1",
    sum(df_input_pivot[col.upper()] for col in HW1)
)
df_input_pivot = df_input_pivot.withColumn("CS_HW2", sum(df_input_pivot[col.upper()] for col in HW2)).join(df_product_master.select("MATERIAL", "Banded"), on = "MATERIAL", how = "left")


# display(df_input_pivot)

# COMMAND ----------

# display(df_input_pivot)

# COMMAND ----------

list_worm_dpname = ["CF CONC. WHITE 20ML", "OMO GOLD 2700 GR", "LIFEBUOY SHAMPOO THICK & SHINY 6G", "SUNLIGHT DWL NATURE ESSENCE 750G", "LIFEBUOYSOAP TOTAL125G" ]

list_expected_dpname = df_input_pivot.select("DPNAME").distinct().rdd.flatMap(lambda x: x).collect()

for i in list_worm_dpname:
  if i not in list_expected_dpname:
    raise Exception(f"{i} is a wrong choice")


# COMMAND ----------

display(df_input_pivot)

# COMMAND ----------

df_input_pivot.repartition(NUMBER_OF_PARTITIONS).write.mode("overwrite").parquet(
    "dbfs:/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/LE_SEC_SALES_YAFSU_FORMAT_PARQUET"
)

# COMMAND ----------

# display(df_input_pivot.count())

# COMMAND ----------

import requests
from bs4 import BeautifulSoup

def scrape_table(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table')  # find the first table on the page

    dt_dict = {"year": [],
               "number_of_days": [],
               "number_of_iso_weeks": [],
               "leap_year_cycle": [],
               "leap_year": []}

    for row in table.find_all('tr'):  # iterate over the rows
        columns = row.find_all('td')  # find each column in each row
        data = [col.text for col in columns]  # get the text from each column
        # print("here is fine")
        if len(data) > 1:
          dt_dict["year"].append(data[0])
          dt_dict["number_of_days"].append(data[1])
          dt_dict["number_of_iso_weeks"].append(data[2])
          dt_dict["leap_year"].append(data[3])
          dt_dict["leap_year_cycle"].append(data[4])
        # print(data)
    return dt_dict
        

# display(pd.DataFrame(scrape_table('https://www.epochconverter.com/years')))


# COMMAND ----------

l = [1,2,3]
print(l[1])

# COMMAND ----------

