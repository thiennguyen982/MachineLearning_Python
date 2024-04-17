# Databricks notebook source
# Import Neccessary Library
## python native library
import os
import re
import sys
import time
from datetime import date, datetime
import pendulum
from pathlib import Path

import numpy as np
import pandas as pd

# Spark Function
import pyspark.pandas as ps
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns
import seaborn.objects as so
import plotly
import plotly.express as px
import plotly.graph_objects as go

# Sklearn
import sklearn
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline 
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, OrdinalEncoder, RobustScaler
from sklearn.model_selection import train_test_split, TimeSeriesSplit, cross_val_predict, cross_validate, GridSearchCV, RandomizedSearchCV

# Tree base model
from sklearn.ensemble import RandomForestRegressor
from lightgbm.sklearn import LGBMRegressor
from xgboost.sklearn import XGBRFRegressor
from catboost import CatBoostRegressor

# statsmodels
# ACF and PACF
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# mlflow
import mlflow
from mlflow import MlflowClient
from mlflow.models import infer_signature
## System metrics
os.environ["MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING"] = "true"
mlflow.enable_system_metrics_logging()
import warnings
warnings.filterwarnings('ignore')


from typing import Optional, Union, List, Dict, Any


# Utils function
MOUNT_DS_PROJECT = "/Workspace/Repos/dao-minh.toan@unilever.com/ds_projects/"
sys.path.append(MOUNT_DS_PROJECT)
from mt_predicted_order.common.sklearn import fa_avg_scorer, fa_sumup_scorer, fa_avg_score, fa_sumup_score, TimeSeriesFE 

from mt_predicted_order.common.sklearn.category_transform import CategoricalTransformer
from mt_predicted_order.common.sklearn.window_ops import RollingMean, RollingMax, RollingSum
from mt_predicted_order.common.mlflow import MlflowService
from mt_predicted_order.common.logger import logger

# Setup SparkSession
spark: SparkSession = SparkSession.builder \
    .appName("HALF_WEEK_MPO_FORECASTING") \
    .getOrCreate()

# Setup MLflow Experiment
EXPERIMENT_NAME = "/Users/dao-minh.toan@unilever.com/HALF_WEEK_MPO_FORECASTING"
mlflowService: MlflowService = MlflowService(EXPERIMENT_NAME)
mlflow.autolog()

print(f"""
VERSION:
    spark: {spark.version}
    sklearn: {sklearn.__version__}
""")

# COMMAND ----------

df_spark: ps.DataFrame = spark.read.format("delta").load("/mnt/adls/mt_feature_store/data_halfweek_fe")
df = df_spark.toPandas()
display(df_spark)

# COMMAND ----------

display(df_spark)

# COMMAND ----------

from ydata_profiling import ProfileReport


report = ProfileReport(df, title="Profiling Report")

report_html = report.to_html()
# displayHTML(report_html)


# COMMAND ----------

print("Dataset shape:", df.shape)
print("Data Types:\n", df.dtypes)
display(df)

# COMMAND ----------

# def plot_missing_values(df):
#     """Plot missing values as a heatmap."""
#     plt.figure(figsize=(12, 6))
#     sns.heatmap(df.isnull(), cbar=False, cmap='viridis')
#     plt.title('Missing Values Heatmap')
#     plt.show()

data = df.isnull().sum().div(len(df)).rename("pct_missing").sort_values(ascending = False).reset_index()
# px.bar(data, y = "index", x = "pct_missing", title = "Missing Value Analysis")
display(data)

# COMMAND ----------

# Count the number of rows in the DataFrame
print("Number of rows:", df_spark.count())
# Count the number of distinct rows in the DataFrame
print("Number of distinct rows:", df_spark.distinct().count())

# COMMAND ----------

# DBTITLE 1,Missing Values Analysis
# Count missing values in each column
df_spark.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in df_spark.columns])


# COMMAND ----------

df_spark.describe().display()

# COMMAND ----------

# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.stat import Correlation

# numerical_columns = [item[0] for item in df_spark.dtypes if item[1].startswith('int') or item[1].startswith('double')]
# vector_col = "numerical_features"
# assembler = VectorAssembler(inputCols=numerical_columns, outputCol=vector_col)
# df_vector = assembler.transform(df_spark).select(vector_col)

# # Compute Pearson correlation matrix
# matrix = Correlation.corr(df_vector, vector_col).collect()[0][0]
# corrmatrix = matrix.toArray().tolist()

# # Display the correlation matrix
# display(corrmatrix)

# COMMAND ----------

# MAGIC %pip install numpy~=1.22
# MAGIC %pip install -U ydata-profiling

# COMMAND ----------

from ydata_profiling import ProfileReport
profile = ProfileReport(df, title="Profiling Report")

# COMMAND ----------

