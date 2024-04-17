# Databricks notebook source
from mlforecast.utils import generate_daily_series


df = generate_daily_series(
    n_series=2,
    max_length=20,
    min_length=20,
    n_static_features=5,
    static_as_categorical=False,
    with_trend=True,
)

df.head()

# COMMAND ----------

HORIZON = 4
# First step: make sure ds sort ascending
df = df.sort_values(by=["ds"])
# transform future, horizon
def transform_future_horizon(group, horizon: int):
    group["future"] = False
    group.iloc[-horizon:, group.columns.get_loc("future")] = True
    group["horizon"] = group["future"].cumsum()
    return group


df = (
    df.groupby("unique_id")
    .apply(transform_future_horizon, horizon=HORIZON)
    .reset_index(drop=True)
)

df

# COMMAND ----------

import pandas as pd
from window_ops.rolling import rolling_mean
from sklearn.pipeline import Pipeline
from lightgbm.sklearn import LGBMRegressor
from typing import List, Dict, Optional, Any

# from mlforecast.lag_transforms import RollingMean

# Define variable
lags = [1, 4, 8]
lag_transforms = {4: ["rolling_mean"]}


class TimeFE:
    def __init__(
        self, model: Pipeline, label: str, lags: List[int], lag_transforms: Optional[Dict[int, Any]] = None,
        remove_key: bool = False,
         horizon: int = 10
    ):
        self.model = model
        self.label = label
        self.horizon = horizon
        self.lags = lags
        self.lag_transforms = lag_transforms
        self.remove_key = remove_key
        self.fit_ts: Optional[pd.DataFrame] = None,

    def fit(self, X, y=None):
        X_transform: pd.DataFrame = self.window_transform(X)
        self.fit_ts = X
        self.fit_ts["future"] = False
        print("Fit columns: ", X_transform.columns)
        self.model.fit(self.auto_cast_type(X_transform), y)
        return self

    def transform(self, X):
        
        return self.horizon_transform(X)
    def horizon_transform(self, X):
        data = X.copy()
        
        if self.fit_ts is not None:
            future_df = data.merge(self.fit_ts, on = ["unique_id", "ds"], how = "outer", suffixes = ("", "_y"))
            future_df = future_df.loc[future_df["future"]!= False]
            if len(future_df) > 0:
                future_df["future"] = True
                data = pd.concat([self.fit_ts, future_df])
                data = data.sort_values(by = ["ds"])
                # Create horizon column
                data = data.groupby("unique_id").apply(self.transform_future_horizon).reset_index(drop=True)

                for step in range(1, int(data.horizon.max()) + 1):
                    data_predict = self.window_transform(data)
                    horizon_predict = self.auto_cast_type(data_predict.loc[data_predict["horizon"] == step, :].drop(columns = ["future", "horizon"]))

                    data.loc[data["horizon"] == step, self.label] = self.model.predict(horizon_predict)
                
                data = data.drop(columns = ["horizon", "future"])
            else:
                print("Go to because future_df = 0")

        else:
            print("Go to without fit_ts")
            
        data = self.window_transform(data)
        X = X.merge(data, on = ["unique_id", "ds"], how = "inner", suffixes = ("", "_y"))
        X = X.drop(columns = [col for col in X.columns if "_y" in col])
        return X
    
    
    def window_transform(self, X):
        data = X.copy()
        data = data.sort_values(by = ["ds"])
        fe_columns = []
        for group in ["unique_id"]:
            for lag in self.lags:
                column_name = f"{self.label}_shift_{lag}"
                data[column_name] = data.groupby(group)[self.label].shift(lag).reset_index(drop = True)
                fe_columns.append(column_name)
        X = X.merge(data[[group, "ds"] + fe_columns], on = [group, "ds"], how = "inner", suffixes= ("", "_y"))
        X = X.drop(columns = [col for col in X.columns if "_y" in col])
        return X
    @staticmethod
    def transform_future_horizon(group) -> pd.DataFrame:
        group["horizon"] = group["future"].cumsum()
        return group
    @staticmethod
    def auto_cast_type(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception as e:
                df[col] = df[col].astype("category")
        return df
            

fe_ts = TimeFE(model = LGBMRegressor(), label = "y", lags = [1, 2])
columns = ['unique_id', 'ds', 'y', 'static_0', 'static_1', 'static_2', 'static_3',
       'static_4']
train = df.iloc[:20]
test = df.iloc[20:]
fe_ts.fit(train.loc[:, columns], train["y"])

fe_ts.transform(test.loc[:, columns])


# COMMAND ----------

# MAGIC %pip install
# MAGIC %pip install
# MAGIC %pip install mlforecast
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# from ydata_profiling import ProfileReport

# profile = ProfileReport(series, tsmode=True, sortby="ds", title="Time-Series EDA")

# COMMAND ----------

from typing import List, Optional
import pandas as pd
import plotly.express as px


class EDAProfile:
    def __init__(
        self,
        data: pd.DataFrame,
        x: Optional[str] = None,
        y: Optional[str] = None,
        color: Optional[str] = None,
        date_features: Optional[str] = None,
    ):
        super().__init__()
        self.data: pd.DataFrame = self.auto_cast_dataframe(
            data, columns=[x, y, color], date_features=date_features
        )
        self.x = x
        self.y = y
        self.color = color

    def bar(self, **kwargs):
        fig = px.bar(
            data_frame=self.data, x=self.x, y=self.y, color=self.color, **kwargs
        )
        return fig

    def line(self, **kwargs):
        fig = px.line(
            data_frame=self.data, x=self.x, y=self.y, color=self.color, **kwargs
        )
        return fig

    @staticmethod
    def auto_cast_dataframe(
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        date_features: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        if columns is None:
            columns = df.columns
        for col in columns:
            try:
                if col in date_features:
                    df[col] = pd.to_datetime(df[col])
                else:
                    df[col] = pd.to_numeric(df[col])
            except Exception as e:
                df[col] = df[col].astype("category")

        return df


EDAProfile(df, x="unique_id", y="y", color="static_3").bar(title="Kaka").show()

# COMMAND ----------

file_path = "/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/2023.47_SL_BACH HOA XANH.xlsx"

import pyspark.pandas as ps

ps.read_excel(file_path, dtype="str")

# COMMAND ----------

