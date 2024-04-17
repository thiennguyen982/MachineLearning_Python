# Databricks notebook source
import pandas as pd
import numpy as np
from scipy.spatial import distance_matrix
from sklearn.metrics.pairwise import cosine_distances
from scipy.stats import ttest_ind
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans, SpectralClustering
from sklearn.metrics import silhouette_score
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame as PySparkDataFrame
from loguru import logger

# COMMAND ----------

import os
import sys
sys.path.append(os.path.abspath('/Workspace/Repos/'))

# COMMAND ----------

from ds_vn.ds_projects.automated_insights.service import generate_features
from ds_vn.ds_core import data_processing_utils

# COMMAND ----------

def calculate_avg_gsv_l3m(date_run: datetime) -> PySparkDataFrame:
    """
    Calculate the average of gsv of last 3 months for the outlet_type calculation
    """
    to_date = date_run - relativedelta(days=1)
    from_date = to_date - relativedelta(months=3)
    master_df = generate_features.load_master_dataset(
        from_date=from_date, to_date=to_date
    )
    result = master_df.groupby("outlet_code", "site_code").agg(
        F.sum(F.col("gross_sales_val")).alias("sum_gsv_l3m")
    )
    result = result.withColumn("avg_gsv_l3m", F.col("sum_gsv_l3m") / 3)
    result = result.select("outlet_code", "site_code", "avg_gsv_l3m")
    return result

# COMMAND ----------

def load_active_outlet_code_gsv_monthly(date_run: datetime) -> PySparkDataFrame:
    """
    Consider the previous month performance -> segment and use for the current month
    For example: 20240101 -> consider the performance of site in the 202312 -> cluser and use the segmentation for the site comparation in Jan 2024
    """
    begin_date = date_run - relativedelta(day=1)
    first_date_prev_month = begin_date - relativedelta(months=1)
    last_date_prev_month = begin_date - relativedelta(days=1)
    master_df = generate_features.load_master_dataset(
        from_date=first_date_prev_month, to_date=last_date_prev_month
    )
    distinct_active_outlets = master_df.groupby("outlet_code", "site_code").agg(
        F.sum(F.col("gross_sales_val")).alias("gsv")
    )
    return distinct_active_outlets

# COMMAND ----------

def form_outlet_type_for_each_outlet_code(df: PySparkDataFrame, date_run: datetime) -> PySparkDataFrame:
    """
    Create a outlet_type column based-on the average gsv last 3 months

    Arguments:
    df: is the dataframe by outlet_code + site_code
    """
    avg_gsv_l3m = calculate_avg_gsv_l3m(date_run=date_run)
    avg_gsv_l3m = avg_gsv_l3m.withColumn(
        "outlet_type",
        F.when(F.col("avg_gsv_l3m") > 500000000, "A")
        .when((F.col("avg_gsv_l3m") > 200000000), "B")
        .when((F.col("avg_gsv_l3m") > 50000000), "C")
        .when((F.col("avg_gsv_l3m") > 20000000), "D")
        .when((F.col("avg_gsv_l3m") > 10000000), "E")
        .when((F.col("avg_gsv_l3m") > 2000000), "F")
        .otherwise("G-H"),
    )
    
    result = df.join(avg_gsv_l3m, on=["outlet_code", "site_code"])
    return result

# COMMAND ----------

def form_master_df(date_run:datetime) -> PySparkDataFrame:
  """
  By outlet_code + site_code
  """
  pre_month_active_outlets = load_active_outlet_code_gsv_monthly(date_run=date_run)
  df = form_outlet_type_for_each_outlet_code(df=pre_month_active_outlets, date_run=date_run)
  return df

# COMMAND ----------

def prepare_data_for_clustering(df: PySparkDataFrame) -> pd.DataFrame:
    """
    Transform to before pivot by site_code
    """
    preprocess_data = df.toPandas()
    # count number of outlet type in site
    total_outlet_by_dt = (
        preprocess_data.groupby(["site_code", "outlet_type"])
        .agg({"outlet_code": "nunique"})
        .reset_index()
    )
    total_outlet_by_dt = total_outlet_by_dt.rename(
        columns={"outlet_code": "num_outlet_by_type_dt"}
    )

    preprocess_data["active"] = preprocess_data["gsv"].apply(
        lambda x: 1 if x > 0 else 0
    )
    preprocess_data = preprocess_data.dropna(subset=["site_code"])
    preprocess_data = (
        preprocess_data.groupby(["site_code", "outlet_type"])
        .agg({"gsv": "sum", "active": "sum"})
        .reset_index()
    )
    preprocess_data.rename(columns={"active": "eco"}, inplace=True)
    preprocess_data = preprocess_data.merge(
        total_outlet_by_dt, on=["site_code", "outlet_type"], how="left"
    )

    # fill missing outlet_type missing in site_code
    dummy_df = pd.DataFrame(
        data={"outlet_type": preprocess_data["outlet_type"].unique()}
    )
    dummy_df = pd.DataFrame(
        data={"site_code": preprocess_data["site_code"].unique()}
    ).merge(dummy_df, how="cross")

    preprocess_data = dummy_df.merge(
        preprocess_data, on=["site_code", "outlet_type"], how="left"
    ).fillna(0)
    # calculate gsv, eco, number of outlet type in site(dt) level
    total_by_dt = (
        preprocess_data.groupby("site_code")
        .agg({"gsv": "sum", "eco": "sum", "num_outlet_by_type_dt": "sum"})
        .reset_index()
    )
    total_by_dt.rename(
        columns={
            "gsv": "total_gsv",
            "eco": "total_eco",
            "num_outlet_by_type_dt": "total_outlet_by_dt",
        },
        inplace=True,
    )
    # calculate proportion
    preprocess_data = preprocess_data.merge(total_by_dt, on=["site_code"], how="left")
    preprocess_data = preprocess_data[preprocess_data["total_gsv"] != 0]
    preprocess_data["coverage"] = (
        preprocess_data["num_outlet_by_type_dt"] / preprocess_data["total_outlet_by_dt"]
    )
    preprocess_data["gsv_contribution"] = (
        preprocess_data["gsv"] / preprocess_data["total_gsv"]
    )
    preprocess_data.fillna(0, inplace=True)
    return preprocess_data

# COMMAND ----------

def pivot_master_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot to be muti dimensional data for Kmeans
    """
    cluster_data = df.pivot(
        index=["site_code", "total_eco", "total_gsv"],
        columns="outlet_type",
        values=["coverage", "gsv_contribution"],
    )
    cluster_data.columns = ["_".join(col) for col in cluster_data.columns]
    cluster_data = cluster_data.reset_index().set_index("site_code")
    return cluster_data

# COMMAND ----------

def normalize_total_eco_gsv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Why normalize? to make the total eco and total gsv in range 0-1. Same as coverage and contribution scale
    """
    scaler = MinMaxScaler()
    df[["total_eco", "total_gsv"]] = scaler.fit_transform(
        df[["total_eco", "total_gsv"]]
    )
    return df

# COMMAND ----------

def fit_clustering_model(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fit a clustering model on the whole set of features
    """
    X = df.copy()
    model = SpectralClustering(
        n_clusters=2, assign_labels="discretize", random_state=42
    )
    model.fit(X)
    return model

# COMMAND ----------

def infer_segment(df: pd.DataFrame, model: SpectralClustering) -> pd.DataFrame:
    test_data = df.copy()
    test_data["label"] = model.labels_
    print("Number of sites by segment: \n{}".format(test_data["label"].value_counts()))
    overall_data = df.merge(test_data[["label"]], on="site_code", how="left")
    return overall_data

# COMMAND ----------

def get_region_mapping(df: pd.DataFrame) -> pd.DataFrame:
    dt_region_df = spark.read.table("distributormaster")
    dt_region_df = dt_region_df.select("site_code", "dt_region").distinct()
    dt_region_df = dt_region_df.toPandas()
    result = df.merge(dt_region_df, how="left", on="site_code")
    result = result.sort_values(by="outlet_type", ascending=True)
    return result

# COMMAND ----------

def generate_final_output(df: pd.DataFrame, df_segment: pd.DataFrame) -> pd.DataFrame:
    """
    After segment by site code -> combine with region and back to the outlet_code + site_code base
    """
    overall_data = df.merge(df_segment[["label"]], on="site_code", how="left")
    overall_data = get_region_mapping(df=overall_data)
    return overall_data

# COMMAND ----------

date_run = datetime(2022,12,1)
master_df = form_master_df(date_run=date_run)
df = prepare_data_for_clustering(df=master_df)
df_pivot = pivot_master_data(df=df)
df_scale = normalize_total_eco_gsv(df=df_pivot)
model = fit_clustering_model(df=df_scale)
df_segment = infer_segment(df=df_scale, model=model)
df_segment = df_segment.reset_index()[["site_code", "label"]]
df_segment.rename(columns={"label": "site_cluster"}, inplace=True)
# final_result = generate_final_output(df=df, df_segment=df_segment)

# COMMAND ----------

df_segment.head()

# COMMAND ----------

df_segment.site_cluster.value_counts()

# COMMAND ----------

df_segment.shape

# COMMAND ----------

def write_inference_result_to_dbfs(df: pd.DataFrame, date_run:datetime) -> None:
  yyyymm = date_run.strftime("%Y%m")
  path = "/dbfs/mnt/adls/ds_vn/service/automated_insights/segmentation/{}".format(yyyymm)
  df.to_parquet(path)
  logger.success("Write inference output of {} successfully".format(yyyymm))

# COMMAND ----------

write_inference_result_to_dbfs(df=df_segment, date_run=date_run)

# COMMAND ----------

