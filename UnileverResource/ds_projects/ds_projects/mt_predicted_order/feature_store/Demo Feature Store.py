# Databricks notebook source
import pandas as pd
 
from pyspark.sql.functions import monotonically_increasing_id, expr, rand
import uuid
 
from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup
 
import mlflow
import mlflow.sklearn
 
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
 

# COMMAND ----------

raw_data = spark.read.load("/databricks-datasets/wine-quality/winequality-red.csv",format="csv",sep=";",inferSchema="true",header="true" )
 
def addIdColumn(dataframe, id_column_name):
    """Add id column to dataframe"""
    columns = dataframe.columns
    new_df = dataframe.withColumn(id_column_name, monotonically_increasing_id())
    return new_df[[id_column_name] + columns]
 
def renameColumns(df):
    """Rename columns to be compatible with Feature Store"""
    renamed_df = df
    for column in df.columns:
        renamed_df = renamed_df.withColumnRenamed(column, column.replace(' ', '_'))
    return renamed_df
 
# Run functions
renamed_df = renameColumns(raw_data)
df = addIdColumn(renamed_df, 'wine_id')
 
# Drop target column ('quality') as it is not included in the feature table
features_df = df.drop('quality')
display(features_df)

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

df = spark.read.table("mt_predicted_order.mt_promotion")
df.columns

# COMMAND ----------



fs.create_table(
  name = "mt_promotion",
  primary_keys = ["Banner"],
  df = df,
  schema = df.schema,
  description = "mt promotion"

)

# COMMAND ----------

