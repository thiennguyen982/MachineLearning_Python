# Databricks notebook source
import os
import pandas as pd
import sys
sys.path.append(os.path.abspath('/Workspace/Repos/'))
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from ds_vn.ds_projects.automated_insights.service import generate_features
from pyspark.sql import DataFrame as PySparkDataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC # Check the overview of site 

# COMMAND ----------

distributor_master = spark.read.table("distributormaster")

# COMMAND ----------

display(distributor_master)

# COMMAND ----------

number_of_site_by_distributor = distributor_master.groupby("dt_code").agg(F.count(F.col("site_code")).alias("count_site_code"))

# COMMAND ----------

display(number_of_site_by_distributor)

# COMMAND ----------

display(number_of_site_by_distributor.summary("count", "min", "25%", "75%","80%", "85%", "90%", "95%", "max"))

# COMMAND ----------

display(number_of_site_by_distributor.groupby("count_site_code").count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Check repeated site buying over time

# COMMAND ----------

to_date = datetime(2023,11,30)
from_date = datetime(2023,11,1)
master_df_202311 = generate_features.load_master_dataset(from_date=from_date, to_date=to_date)

# COMMAND ----------

to_date = datetime(2023,10,31)
from_date = datetime(2023,10,1)
master_df_202310 = generate_features.load_master_dataset(from_date=from_date, to_date=to_date)

# COMMAND ----------

check_10 = master_df_202310.select("site_code").distinct()

# COMMAND ----------

check_11 = master_df_202311.select("site_code").distinct()

# COMMAND ----------

check_10.count()

# COMMAND ----------

check_11.count()

# COMMAND ----------

check_10.join(check_11, on="site_code").count()

# COMMAND ----------

to_date = datetime(2023,4,30)
from_date = datetime(2023,4,1)
master_df_202304 = generate_features.load_master_dataset(from_date=from_date, to_date=to_date)

to_date = datetime(2023,5,31)
from_date = datetime(2023,5,1)
master_df_202305 = generate_features.load_master_dataset(from_date=from_date, to_date=to_date)

print("202304 number of site: ", master_df_202304.select("site_code").distinct().count())
print("202305 number of site: ", master_df_202305.select("site_code").distinct().count())
print("202304 in 202305 number of site: ", master_df_202304.select("site_code").distinct().join(master_df_202305.select("site_code").distinct(), on="site_code").count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Calculate the performance mean

# COMMAND ----------

# MAGIC %md
# MAGIC calculate the performance so that to compare with the performance of the individual sites

# COMMAND ----------

date_run = datetime(2023,12,20)
begin_date_curr_month = date_run - relativedelta(day=1)
last_date_pre_month = begin_date_curr_month - relativedelta(days=1)
from_date = date_run - relativedelta(days=3)
to_date = date_run
master_df = generate_features.load_master_dataset(from_date, to_date)

# COMMAND ----------

display(master_df)

# COMMAND ----------

def get_region_mapping(df: PySparkDataFrame) -> PySparkDataFrame:
    dt_region_df = spark.read.table("distributormaster")
    dt_region_df = dt_region_df.select("site_code", "dt_region").distinct()
    result = df.join(dt_region_df, how="left", on="site_code")
    return result

# COMMAND ----------

def load_site_cluster(date_run: datetime) -> PySparkDataFrame:
  """
  Load the result from site segmentation phase
  """
  yyyymm = date_run.strftime("%Y%m")
  path = "/mnt/adls/ds_vn/service/automated_insights/segmentation/{}".format(yyyymm)
  df = spark.read.parquet(path)
  return df

# COMMAND ----------

def load_master_df_with_extra_info(date_run: datetime) -> PySparkDataFrame:
  from_date = date_run - relativedelta(day=1)
  to_date = date_run
  master_df = generate_features.load_master_dataset(from_date, to_date)
  df = get_region_mapping(master_df)
  site_segment = load_site_cluster(date_run)
  df = df.join(site_segment, on="site_code", how="left")
  return df

# COMMAND ----------

def cal_mtd_mean_performance(df:PySparkDataFrame) -> PySparkDataFrame:
  df.createOrReplaceTempView("sql_df")
  result = spark.sql(
    """
    select dt_region, channel, small_c, site_cluster, 
    sum(gross_sales_val) as gsv, count(distinct outlet_code) as eco,
    count(distinct cd_dp_name) as assortment,
    count(distinct site_code) as number_of_site
    from sql_df
    group by dt_region, channel, small_c, site_cluster
    order by dt_region, channel, small_c, site_cluster
    """
  )
  result = result.withColumn("gsv_per_assortment", F.col("gsv")/ F.col("assortment"))
  result = result.withColumn("eco_per_site", F.col("eco")/ F.col("number_of_site"))
  result = result.withColumn("assortment_per_site", F.col("assortment")/ F.col("number_of_site"))
  result = result.drop("eco", "assortment", "number_of_site")
  return result

# COMMAND ----------

def get_sply_gsv_performance(
    df: PySparkDataFrame, date_run: datetime, keys: list
) -> PySparkDataFrame:
    same_date_ly = date_run - relativedelta(months=12)
    master_df = load_master_df_with_extra_info(same_date_ly)
    group = master_df.groupby(*keys).agg(
        F.sum(F.col("gross_sales_val")).alias("gsv_ly")
    )
    result = df.join(group, on=keys)
    return result

# COMMAND ----------

def cal_gsv_mtd_growth(
    df: PySparkDataFrame,
    base_column_name: str,
    ly_column_name: str,
    output_column_name: str,
) -> PySparkDataFrame:
    df = df.withColumn(
        output_column_name, F.col(base_column_name) / F.col(ly_column_name) - 1
    )
    return df

# COMMAND ----------

date_run = datetime(2023,12,25)
master_df_extra = load_master_df_with_extra_info(date_run=date_run)
mean_df = cal_mtd_mean_performance(df=master_df_extra)
mean_df = get_sply_gsv_performance(df=mean_df, date_run=date_run, keys=["dt_region", "channel", "small_c", "site_cluster"])
mean_df = cal_gsv_mtd_growth(mean_df, base_column_name="gsv", ly_column_name="gsv_ly", output_column_name="mean_gsv_mtd_growth")

# COMMAND ----------

display(mean_df)

# COMMAND ----------

mean_df.count()

# COMMAND ----------

display(mean_df.groupby("dt_region", "channel", "small_c").count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Calculate the individual performance

# COMMAND ----------

date_run = datetime(2023,12,25)
master_df_extra = load_master_df_with_extra_info(date_run=date_run)

# COMMAND ----------

display(master_df_extra)

# COMMAND ----------

def cal_mtd_individual_performance(df:PySparkDataFrame) -> PySparkDataFrame:
  df.createOrReplaceTempView("tmp_df")
  result = spark.sql(
    """
    select site_code, dt_region, channel, small_c, site_cluster, 
    sum(gross_sales_val) as indi_gsv, count(distinct outlet_code) as indi_eco, 
    count(distinct cd_dp_name) as indi_assortment
    from tmp_df
    group by site_code, dt_region, channel, small_c, site_cluster
    order by site_code, dt_region, channel, small_c, site_cluster
    
    """
  )
  result = result.withColumn("indi_gsv_per_assortment", F.col("indi_gsv")/ F.col("indi_assortment"))
  return result

# COMMAND ----------

date_run = datetime(2023,12,25)
master_df_extra = load_master_df_with_extra_info(date_run=date_run)
indi_df = cal_mtd_individual_performance(df=master_df_extra)
indi_df = get_sply_gsv_performance(df=indi_df, date_run=date_run, keys=["site_code","dt_region", "channel", "small_c", "site_cluster"])
indi_df = cal_gsv_mtd_growth(indi_df, base_column_name="indi_gsv", ly_column_name="gsv_ly", output_column_name="indi_gsv_mtd_growth")

# COMMAND ----------

display(indi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine mean_df with indi_df

# COMMAND ----------

def combine_mean_with_indi_df(mean_df: PySparkDataFrame, indi_df: PySparkDataFrame) -> PySparkDataFrame:
  df = indi_df.join(mean_df, on=["dt_region", "channel", "small_c", "site_cluster"])
  return df

# COMMAND ----------

def create_growth_better_or_less_than_region(df: PySparkDataFrame) -> PySparkDataFrame:
    df = df.withColumn(
        "is_growth_better",
        F.when(
            F.col("indi_gsv_mtd_growth") > F.col("mean_gsv_mtd_growth"), "better"
        ).otherwise("worse"),
    )
    return df

# COMMAND ----------

df_combine = combine_mean_with_indi_df(mean_df=mean_df, indi_df=indi_df)

# COMMAND ----------

display(df_combine)

# COMMAND ----------

df_combine = create_growth_better_or_less_than_region(df_combine)

# COMMAND ----------

display(df_combine)

# COMMAND ----------

df_combine.count()

# COMMAND ----------

def normalize_components(df: pd.DataFrame):
    from sklearn.preprocessing import MinMaxScaler
    ori_df = df.copy()
    
    keys=["site_code","dt_region", "channel", "small_c", "site_cluster"]
    scaler = MinMaxScaler()
    cols_to_normalize = [
        "indi_eco",
        "indi_assortment",
        "indi_gsv_per_assortment",
        "eco_per_site",
        "assortment_per_site",
        "gsv_per_assortment",
    ]
    output_columns_name = [str(i + "_normalized") for i in cols_to_normalize]
    df = df[cols_to_normalize]
    df[cols_to_normalize] = scaler.fit_transform(df[cols_to_normalize])
    df.columns = output_columns_name
    result = pd.concat([ori_df, df], axis=1)
    return result

# COMMAND ----------

def calculate_gap_for_each_component(df: pd.DataFrame) -> pd.DataFrame:
  indi_components = ["indi_eco_normalized", "indi_assortment_normalized", "indi_gsv_per_assortment_normalized"]
  mean_components = ["eco_per_site_normalized", "assortment_per_site_normalized", "gsv_per_assortment_normalized"]
  output_name = ["eco", "assortment", "gsv_per_assortment"]
  result = df.copy()
  for i in range(len(indi_components)):
    result["gap_{}".format(output_name[i])] = df[indi_components[i]]/df[mean_components[i]] - 1
  return result

# COMMAND ----------

# Create a custom function to sort column names based on values
def sort_columns_by_values(row):
    sorted_columns = sorted(row.items(), key=lambda x: x[1], reverse=True)
    return [col for col, _ in sorted_columns]

# COMMAND ----------

def sort_components_to_focus(df: pd.DataFrame) -> pd.DataFrame:
  ori_df = df.copy()
  gap_columns = ["gap_eco", "gap_assortment", "gap_gsv_per_assortment"]
  df = df[gap_columns]
  df['order_components_focus'] = df.apply(sort_columns_by_values, axis=1)
  
  df = df[['order_components_focus']]

  result = pd.concat([ori_df, df], axis=1)
  return result  

# COMMAND ----------

def compare_components(df: PySparkDataFrame) -> pd.DataFrame:
    df = df.toPandas()
    df = normalize_components(df)
    df = calculate_gap_for_each_component(df)
    df = sort_components_to_focus(df)
    return df

# COMMAND ----------

date_run = datetime(2023,12,25)
master_df_extra = load_master_df_with_extra_info(date_run=date_run)
indi_df = cal_mtd_individual_performance(df=master_df_extra)
indi_df = get_sply_gsv_performance(df=indi_df, date_run=date_run, keys=["site_code","dt_region", "channel", "small_c", "site_cluster"])
indi_df = cal_gsv_mtd_growth(indi_df, base_column_name="indi_gsv", ly_column_name="gsv_ly", output_column_name="indi_gsv_mtd_growth")

df_combine = combine_mean_with_indi_df(mean_df=mean_df, indi_df=indi_df)
df_combine = create_growth_better_or_less_than_region(df_combine)

result = compare_components(df_combine)

# COMMAND ----------

display(result)

# COMMAND ----------

