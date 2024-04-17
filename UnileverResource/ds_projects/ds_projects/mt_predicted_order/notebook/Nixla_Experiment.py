# Databricks notebook source
from mlforecast.utils import generate_daily_series


series = generate_daily_series(
    n_series=20,
    max_length=100,
    n_static_features=5,
    static_as_categorical=False,
    with_trend=True
)
series.head()

# COMMAND ----------

from mlforecast import MLForecast
from mlforecast.lag_transforms import ExpandingMean, RollingMean
from mlforecast.target_transforms import Differences
from sklearn.ensemble import RandomForestRegressor

fcst = MLForecast(
    models = [RandomForestRegressor()],
    freq = "D",
    lags = [7, 14],
    lag_transforms = {
        1: [ExpandingMean()],
        7: [RollingMean(window_size = 28)]
    },
    date_features =["dayofweek"]
)

# COMMAND ----------

from prophet import Prophet
import pandas as pd
from typing import Any, Union, List, Dict


class SklearnTS(TSFeature):
    def __init__(self, regressor, freq: str = "D"):
        super().__init__(freq = freq)
        self.regressor = regressor
    def fit(self, X, y = None):
        self.regressor.fit(X, y)
        return self
    def predict(self, X):
        return self.regressor.predict(X)

class TSForecasting:
    _model: Dict[int, Any] = {
        "prophet": ProphetTS,
        "sklearn": SklearnTS  
    }
    def __init__(self, model_name: str = "prophet", freq = "D") -> None:
        self.model = self._model[model_name](freq = freq)
    def fit(self, X, y = None):
        self.model.fit(X, y)
        return self
    def predict(self, X = None, horizon: int = 10):
        forecast = self.model.predict(X, horizon = horizon)
        return forecast

ts_forecasting = TSForecasting()
ts_forecasting.fit(series).predict(horizon = 10)


# COMMAND ----------

from mlforecast import MLForecast
from mlforecast.utils import generate_series, generate_prices_for_series, generate_daily_series
fcst = MLForecast(
    models= [RandomForestRegressor()],
    freq = "D",
    lags = [1],
    date_features = ["dayofweek"]
)

series = generate_series(equal_ends=True, n_series=1, max_length=10, min_length=10)
series

# COMMAND ----------

from mlforecast.lag_transforms import RollingMean, ExpandingStd

fcst = MLForecast(
    models = [RandomForestRegressor()],
    freq = "D",
    lags = [1],
    lag_transforms={
        1: [ExpandingMean()],
        2: [RollingMean(window_size=1)]
    },
    date_features= ["dayofweek"]
)
fcst.preprocess(series, dropna = False).head(10)

# COMMAND ----------

from sklearn.pipeline import Pipeline
from typing import List, Optional, Union
class TSFeatureEngineering:
    def __init__(self,
                 model: Pipeline,
                 label: str,
                 freq: str,
                 lags: List[int], 
                 lag_transformers: Any,
                 horizon: int,
                 categorical_columns: List[str] = None
                 ):
        self.model = model
        self.freq = freq
        self.lags = lags 
        self.lag_transformers = lag_transformers
        self.label = label
        self.horizon = horizon
        self.categorical_columns = categorical_columns
    

    def fit(self, X, y = None):
        train = X.query("future = 'N'")
        train = self._lag_transforms(train)
        X_train = train.drop(columns = ["future", "ds", "y"])
        y_train = train["y"]
        if self.categorical_columns is None:
            self.categorical_columns = []
            for col in X_train.columns:
                try:
                    X_train[col] = pd.to_numeric(X_train[col])
                except Exception as e:
                    X_train[col] = X_train[col].astype("category")
                    self.categorical_columns.append(col)
        
        self.model.fit(X_train, y_train)
        return self
    def transform(self, X):
        for step in range(self.horizon):
            X = self._lag_transforms(X)
            for col in self.categorical_columns:
                X[col] = X[col].astype("category")
            X.loc[X["horizon"] == step, self.label] = self.model.predict(X.loc[X["horizon"] == step].drop(columns = ["future", "ds", "y"]))
        return X 
    
    def _lag_transforms(self, X):
        data = X.copy()
        data = data.sort_values(by = ["ds"])
        fe_columns = []
        for group in ["unique_id"]:
            for lag in self.lags:
                column_name = f"{self.label}_lag_{lag}"
                data[column_name] = data.groupby(group)[self.label].shift(lag).reset_index(drop = True)
                fe_columns.append(column_name)
                if lag in self.lag_transformers.keys():
                    column_name = f"{self.label}_rolling_mean_28"
                    data[column_name] = data.groupby(group)[self.label].shift(lag).rolling(window=28).reset_index(drop = True)
                    fe_columns.append(column_name)
            X = X.merge(data[fe_columns], on = [group, "ds"], how = "left")
        return X



from mlforecast.utils import generate_daily_series


series = generate_daily_series(
    n_series=20,
    max_length=100,
    n_static_features=5,
    static_as_categorical=False,
    with_trend=True
)
series["future"] = "N"
series["horizon"] = -1
series.head()
series["ds"].max()

# COMMAND ----------

df = pd.DataFrame({'A': [1, 1, 2, 2],
                   'B': [1, 2, 3, 4],
                   'C': [0.362, 0.227, 1.267, -0.562]})

# COMMAND ----------

df.head()

# COMMAND ----------

from window_ops.rolling import rolling_mean

df.groupby("A")["B"].shift(1).rolling(window=1).sum().reset_index()

# COMMAND ----------

