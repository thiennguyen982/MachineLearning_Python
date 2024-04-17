# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Sale analysis
# MAGIC with sale as (
# MAGIC select 
# MAGIC to_date(billing_date) as ds,
# MAGIC banner,
# MAGIC brand,
# MAGIC region,
# MAGIC dp_name,
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC
# MAGIC cast(est_demand_cs as bigint) as est_demand,
# MAGIC category,
# MAGIC u_price_cs as u_price
# MAGIC -- Promotion Features
# MAGIC -- promotion_type
# MAGIC from mt_predicted_order.mt_sale
# MAGIC )
# MAGIC
# MAGIC , sale_final as (
# MAGIC select *
# MAGIC from sale
# MAGIC )
# MAGIC
# MAGIC -- Promotion analysis
# MAGIC , promotion as (
# MAGIC select 
# MAGIC banner,
# MAGIC dp_name,
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC
# MAGIC on_off_post,
# MAGIC order_start_date,
# MAGIC order_end_date
# MAGIC
# MAGIC
# MAGIC from mt_predicted_order.mt_promotion
# MAGIC )
# MAGIC , promotion_explode as (
# MAGIC select 
# MAGIC explode(sequence(
# MAGIC     case when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_start_date) else null end,
# MAGIC     case when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_end_date) else null end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC banner,
# MAGIC dp_name,
# MAGIC ulv_code,
# MAGIC on_off_post,
# MAGIC order_start_date,
# MAGIC order_end_date
# MAGIC from promotion
# MAGIC )
# MAGIC , promotion_final as (
# MAGIC select *
# MAGIC from promotion_explode
# MAGIC )
# MAGIC -- master data
# MAGIC
# MAGIC -- Price sensitivity
# MAGIC , price_change as (
# MAGIC select 
# MAGIC billing_date,
# MAGIC banner,
# MAGIC dp_name,
# MAGIC start_date,
# MAGIC end_date,
# MAGIC relaunch,
# MAGIC activity_type,
# MAGIC price_change
# MAGIC from mt_predicted_order.mt_price_change
# MAGIC )
# MAGIC , price_change_explode as (
# MAGIC select 
# MAGIC explode(sequence(
# MAGIC     case when to_date(start_date) <= to_date(end_date) then to_date(start_date) else null end,
# MAGIC     case when to_date(start_date) <= to_date(end_date) then to_date(end_date) else null end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC banner,
# MAGIC dp_name,
# MAGIC billing_date,
# MAGIC start_date,
# MAGIC end_date,
# MAGIC relaunch,
# MAGIC activity_type,
# MAGIC price_change
# MAGIC from price_change
# MAGIC )
# MAGIC , price_change_final as (
# MAGIC select *
# MAGIC from price_change_explode
# MAGIC )
# MAGIC -- Calendar
# MAGIC , calendar_final as (
# MAGIC select
# MAGIC to_date(billing_date) as ds,
# MAGIC year,
# MAGIC holiday,
# MAGIC weekday,
# MAGIC weekofmonth
# MAGIC from mt_predicted_order.mt_calendar
# MAGIC )
# MAGIC
# MAGIC -- Product Information
# MAGIC , product_info as (
# MAGIC select *
# MAGIC
# MAGIC from mt_predicted_order.mt_product_master
# MAGIC ) 
# MAGIC
# MAGIC select 
# MAGIC sale.* ,
# MAGIC promotion.* except (ds, banner, dp_name, ulv_code),
# MAGIC price.* except (ds, banner, dp_name),
# MAGIC calendar.* except (ds),
# MAGIC -- feature engineering
# MAGIC mean(est_demand) over (partition by sale.banner, region order by unix_date(sale.ds) range between 7 preceding and 1 preceding) as est_demand_d7
# MAGIC from sale_final sale
# MAGIC left join promotion_final promotion on sale.ds = promotion.ds and sale.banner = promotion.banner and sale.ulv_code = promotion.ulv_code
# MAGIC left join price_change_final price on sale.ds = price.ds and sale.banner = price.banner
# MAGIC left join calendar_final calendar on sale.ds = calendar.ds
# MAGIC where 1=1
# MAGIC and sale.est_demand is not null
# MAGIC and sale.ds >= '2022-01-01'
# MAGIC and sale.banner = "SAIGON COOP"
# MAGIC -- and sale.dp_name = "Sunlight Lemon 750G"
# MAGIC

# COMMAND ----------

df_spark = _sqldf.pandas_api()
df_spark.shape

# COMMAND ----------

df_spark["on_off_post"].unique()

# COMMAND ----------

df_spark.head()

# COMMAND ----------

# import matplotlib.pyplot as plt
# import seaborn as sns
# import plotly.express as px

# data = df.groupby("ds")["est_demand"].sum().reset_index()
# px.line(data_frame= data, x = "ds", y = "est_demand")

# COMMAND ----------

df_final = df_spark.to_pandas()

# COMMAND ----------

# Import Neccessary Library
import os
# Setup environment

# os.environ["PYSPARK_PYTHON"] = "C:\\Users\\Dao-Minh.Toan\\AppData\\Local\\anaconda3\\python.exe"

import re
import sys

from datetime import date
from pathlib import Path
import numpy as np
import pandas as pd

# Spark Function
import pyspark.pandas as ps
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession

# Visualization
import matplotlib.pyplot as plt

import seaborn as sns
import seaborn.objects as so
import plotly
import plotly.express as px
import plotly.graph_objects as go


from sklearn.pipeline import Pipeline 
from sklearn.compose import ColumnTransformer

# Preprocessing
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder

# Tree base model
from sklearn.ensemble import RandomForestRegressor
from lightgbm.sklearn import LGBMRegressor
from xgboost.sklearn import XGBRFRegressor
from catboost import CatBoostRegressor

# model selection
from sklearn.model_selection import train_test_split, TimeSeriesSplit, cross_val_predict, cross_validate

# statsmodels
# ACF and PACF
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

from typing import Optional, Union, List

# COMMAND ----------

label = "est_demand"


# COMMAND ----------

# Correlation
plt.figure(figsize=(25, 10))
corr = df_final.select_dtypes(include='number').corr()
number_of_columns = 10
corr = corr.sort_values(by = label, ascending=False).iloc[:number_of_columns, :number_of_columns]

# corr.head()
mask = np.triu(np.ones_like(corr, dtype=bool))

ax = sns.heatmap(corr, mask=mask, xticklabels=corr.columns, yticklabels=corr.columns, annot=True, linewidths=.2, cmap='coolwarm', vmin=-1, vmax=1)

# COMMAND ----------

# Train Test Validation
df_final = df_final.dropna(how = "all", subset=[label])
df_final = df_final.sort_values(by = ["ds"])
df_final = df_final.reset_index()

sample_size = df_final.shape[0]
test_size= 0.2
df_train, df_test = df_final.iloc[:int(sample_size*(1- test_size))], df_final.iloc[int(sample_size*(1-test_size)):]
print(f"""
train:  
  size: {df_train.shape} 
test:
  size: {df_test.shape}
""")

# COMMAND ----------

numeric = []
category = []
for col in df_final.columns:
  try:
    pd.to_numeric(df_final[col])
    numeric.append(col)
  except:
    category.append(col)
print(f"numeric: {numeric}, category: {category}")
print("Numeric: ")
for col in numeric:
  print(f'"{col}",')
print("Category: ")
for col in category:
  print(f'"{col}",')


# COMMAND ----------

label = "est_demand"

numerical_columns = [
"u_price",
"start_date",
"end_date",
"relaunch",
"activity_type",
"price_change",
"year",
"weekday",
"weekofmonth",
"est_demand_d7",
]

categorical_columns = [
"banner",
"brand",
"region",
# "dp_name",
"category",
"on_off_post",
"holiday"
]
for col in numerical_columns:
    df_train[col] = pd.to_numeric(df_train[col])
    df_test[col] = pd.to_numeric(df_test[col])

# COMMAND ----------

# Model pipeline

numerical_preprocessing = Pipeline(steps = [
  ("imputer", SimpleImputer()),
])

categorical_preprocessing = Pipeline(steps = [
  ("imputer", SimpleImputer(strategy="most_frequent")),
  ("encoder", OneHotEncoder(handle_unknown = "ignore"))
])

combine_preprocessing = ColumnTransformer(transformers=[
        ("numerical", numerical_preprocessing, numerical_columns),
        ("categorial", categorical_preprocessing, categorical_columns)
    ],
                                          verbose_feature_names_out = False,
                                          n_jobs=-1
                                          )
pipe = Pipeline(steps = [
  ("combine", combine_preprocessing),
  ("estimator", RandomForestRegressor())
])
pipe

# COMMAND ----------

cv_results = cross_validate(
  estimator=pipe,
  X = df_train.loc[:, numerical_columns + categorical_columns],
  y = df_train[label],
  scoring="neg_mean_absolute_percentage_error",
  cv = TimeSeriesSplit(n_splits=2),
  return_estimator=True,
  return_train_score=True
)

print(f"""
train_score: {cv_results["train_score"]}, avg_train_score: {np.mean(cv_results["train_score"])}
test_score: {cv_results["test_score"]}, avg_test_score: {np.mean(cv_results["test_score"])}

cv_results: {cv_results} 
""")

# COMMAND ----------

best_pipe = cv_results["estimator"][-1]

ft = pd.DataFrame()
# best_pipe = cv_results["estimator"][np.argmax(cv_results["test_auc"])]
best_pipe = cv_results["estimator"][-1]
ft["features"] = best_pipe[-2].get_feature_names_out()
ft["feature_importances"] = best_pipe[-1].feature_importances_

(so
 .Plot(ft.sort_values(by = ["feature_importances"], ascending=False), y = "features", x = "feature_importances")
 .add( so.Bars())
 .layout(size=(8, 10))
)
# px.bar(
#   data_frame= ft.sort_values(by = ["feature_importances"], ascending=True),
#   y = "features",
#   x = "feature_importances"
# )

# COMMAND ----------

df_train["prediction"] = best_pipe.predict(df_train.loc[:, numerical_columns + categorical_columns])
df_test["prediction"] = best_pipe.predict(df_test.loc[:, numerical_columns + categorical_columns])

# COMMAND ----------

banner = "SAIGON COOP"
region = "V101"
dp_name = 'Clear Shampoo Men Cool Sport 370G'

df_train_viz = df_train.query(f"(banner == '{banner}' & region == '{region}' & dp_name == '{dp_name}')")
df_test_viz = df_test.query(f"(banner == '{banner}' & region == '{region}' & dp_name == '{dp_name}')")

fig = go.Figure([
    go.Scatter(name = "train y_true", x = df_train_viz["ds"], y = df_train_viz[label]),
    go.Scatter(name = "train y_pred", x = df_train_viz["ds"], y = df_train_viz["prediction"]),

    go.Scatter(name = "test y_true", x = df_test_viz["ds"], y = df_test_viz[label]),
    go.Scatter(name = "test y_pred", x = df_test_viz["ds"], y = df_test_viz["prediction"]),
])
fig.show()

# COMMAND ----------

df_train.head()

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(to_date(billing_date))
# MAGIC from mt_predicted_order.mt_sale 
# MAGIC where 1=1
# MAGIC and to_date(billing_date) >= '2021-01-01'
# MAGIC and banner = "SAIGON COOP"
# MAGIC and dp_name = "Clear Shampoo Men Cool Sport 370G"

# COMMAND ----------

from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd

df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gapminder_unfiltered.csv')

app = Dash("demo")
app.layout = html.Div([
    html.H1(children='Title of Dash App', style={'textAlign':'center'}),
    dcc.Dropdown(df.country.unique(), 'Canada', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
])

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = df[df.country==value]
    return px.line(dff, x='year', y='pop')

# app.run(port = "8051", jupyter_mode="external")
app.run(mode = "external", debug = True)

# COMMAND ----------

