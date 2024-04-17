# Databricks notebook source
import pyspark.pandas as ps
import pandas as pd
# Read dataset
df_spark: ps.DataFrame = ps.read_delta("/mnt/adls/mt_feature_store/data_tp_halfweek")
df = df_spark.to_pandas()

# Reformat to TS
df = df.sort_values(by = ["ds"])

display(df)

# COMMAND ----------

df = df.set_index(["unique_id", "ds"])
df.loc[df.index.get_level_values(0) == "big_c|v103|cf. intense care indoor pou 3.2l"].head()

# COMMAND ----------

# Check stationary
from statsmodels.tsa.stattools import adfuller
from sktime.param_est.stationarity import StationarityADF
def perform_adf_test(series):
    result = adfuller(series, autolag='AIC')
    adf_stat, p_value, usedlag, nobs, critical_values, icbest = result
    return {
        'ADF Statistic': adf_stat,
        'p-value': p_value,
        'Critical Values': critical_values,
    }

adf_results = []

for entity, entity_data in df.groupby(level=0):
    adf_result = perform_adf_test(entity_data["est_demand"])  # Assuming the time series values are in the first column
    adf_result["unique_id"] = entity
    adf_results.append(adf_result)

df_adf = pd.DataFrame(adf_results)
df_adf["stationary"] = df_adf["p-value"] <= 0.05
print(f"Number of timeseries that stationary: {df_adf['stationary'].sum() / len(df_adf)}")
display(df_adf)

# COMMAND ----------

df_adf.loc[df_adf["stationary"] == False]

# COMMAND ----------

import matplotlib.pyplot as plt
import statsmodels.api as sm


for idx, (entity, entity_data) in enumerate(df.groupby(level=0)):
    
    if entity in df_adf.query("stationary==True")["unique_id"].unique():
        print(f"entity: {entity}, idx: {idx}")
        fig, ax = plt.subplots(1, 2, figsize=(18, 6))
        sm.graphics.tsa.plot_acf(entity_data.loc[:, "est_demand"], lags=50, ax= ax[0])
        sm.graphics.tsa.plot_pacf(entity_data.loc[:, "est_demand"], lags=50, ax = ax[1])
        fig.suptitle('ACF & PACF with Stationary')
        plt.show()
        break

for idx, (entity, entity_data) in enumerate(df.groupby(level=0)):
    
    if entity in df_adf.query("stationary==False")["unique_id"].unique() :
        print(f"entity: {entity}, idx: {idx}")
        fig, ax = plt.subplots(1, 2, figsize=(18, 6))
        sm.graphics.tsa.plot_acf(entity_data.loc[:, "est_demand"], lags=50, ax= ax[0])
        sm.graphics.tsa.plot_pacf(entity_data.loc[:, "est_demand"], lags=50, ax = ax[1])
        fig.suptitle('ACF & PACF with Non-Stationary')
        plt.show()
        break
# ax.set_title('ACF for Estimated Demand')


# COMMAND ----------

# Target Transform
from sktime.transformations.series.boxcox import LogTransformer
from sktime.transformations.series.difference import Differencer
from sktime.transformations.series.fourier import FourierTransform
from sktime.transformations.panel.slope import SlopeTransformer
df["log1p"] = LogTransformer(offset = 1).fit_transform(df[["est_demand"]])
df["differencer"] = Differencer().fit_transform(df[["est_demand"]])
df["fourier"] = FourierTransform().fit_transform(df[["est_demand"]])
df["slope"] = SlopeTransformer().fit_transform(df[["est_demand"]])
display(df.reset_index())

# COMMAND ----------

