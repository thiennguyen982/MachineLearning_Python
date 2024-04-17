# Databricks notebook source
# MAGIC %pip install mlforecast
# MAGIC %pip install pmdarima
# MAGIC %pip install sktime

# COMMAND ----------

from mlforecast.utils import generate_daily_series
import pandas as pd
from sktime.forecasting.base import ForecastingHorizon
from sktime.forecasting.model_selection import temporal_train_test_split
from sktime.forecasting.naive import NaiveForecaster
from sktime.utils._testing.hierarchical import _bottom_hier_datagen
from sktime.datatypes import convert_to


df = generate_daily_series(
    n_series=2,
    min_length = 20,
    max_length=20,
    n_static_features=2,
    static_as_categorical=False,
    with_trend=True,
)

df["ds"] = pd.to_datetime(df["ds"]).dt.date
# df = df.groupby(["unique_id", "ds"])[["y", "static_0"]].sum()
df = df.set_index(["unique_id", "ds"])
df.index = df.index.set_levels(pd.to_datetime(df.index.levels[1]), level=1)
df["category"] = ["A"]*20 + ["B"]*20
df.head(40)


# COMMAND ----------

# df = pd.DataFrame({
#     "unique_id": ["id_0", "id_0", "id_0", "id_0", "id_0", "id_0", "id_0", "id_0", "id_0", "id_0", "id_1", "id_1", "id_1", "id_1", "id_1", "id_1", "id_1", "id_1", "id_1", "id_1"],
#     # "ds": ["2024-01-02", "2024-01-03", "2024-01-05", "2024-01-08", "2024-01-11","2024-01-13","2024-01-15","2024-01-18","2024-01-21","2024-01-24"],
#     "ds": [
#         "2024-01-02", 
#         "2024-01-05", 
#         "2024-01-07", 
#         "2024-01-09", 
#         "2024-01-13", 
#         "2024-01-15", 
#         "2024-01-18", 
#         "2024-01-20", 
#         "2024-01-22", 
#         "2024-01-25", 
#         "2024-01-02", 
#         "2024-01-05", 
#         "2024-01-07", 
#         "2024-01-09", 
#         "2024-01-13", 
#         "2024-01-15", 
#         "2024-01-18", 
#         "2024-01-20", 
#         "2024-01-22", 
#         "2024-01-25", 
#            ],
#     # "ds": ["2024-01-02", 
#     #        "2024-01-03", 
#     #        "2024-01-04", 
#     #        "2024-01-05", 
#     #        "2024-01-06", 
#     #        "2024-01-07", 
#     #        "2024-01-08", 
#     #        "2024-01-09", 
#     #        "2024-01-10", 
#     #        "2024-01-11", 
#     #        "2024-01-12", 
#     #        "2024-01-13", 
#     #        "2024-01-14", 
#     #        "2024-01-15", 
#     #        "2024-01-16", 
#     #        "2024-01-17", 
#     #        "2024-01-18", 
#     #        "2024-01-19", 
#     #        "2024-01-20", 
#     #        "2024-01-21"
#     #        ],
#     "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
#     }
# )

# ds2index = {ds: idx for idx, ds in enumerate(df["ds"].unique())}
# def transform(lst):
#     lst["ds"] = lst["ds"].replace(ds2index)
#     return lst.loc[:, ["ds", "y"]]

# df = df.groupby(["unique_id"])[["ds", "y"]].apply(lambda x: transform(x)).reset_index()
# # df["ds"] = pd.to_datetime(df["ds"]).dt.date
# df = df.groupby(["unique_id", "ds"])[["y"]].sum()
# # df.index = df.index.set_levels(pd.to_datetime(df.index.levels[1]), level=1)
# df.head(10)

# COMMAND ----------

# y_train, y_test, X_train, X_test = temporal_train_test_split(df["y"], df.drop(columns = ["y"]))
X = df.drop(columns = ["y"])
y = df[["y"]]
y_train, y_test, X_train, X_test = temporal_train_test_split(y, X, test_size = 6)
X_train

# COMMAND ----------

from sktime.forecasting.compose import make_reduction
from sklearn.neighbors import KNeighborsRegressor
from sktime.utils.plotting import plot_series
from sktime.forecasting.arima import ARIMA
from sktime.forecasting.compose import TransformedTargetForecaster, ForecastingPipeline
from sktime.transformations.series.detrend import Deseasonalizer
from sktime.forecasting.compose import make_reduction
from sktime.transformations.series.exponent import ExponentTransformer
from lightgbm.sklearn import LGBMRegressor
from sktime.transformations.series.date import DateTimeFeatures
from sktime.transformations.series.boxcox import LogTransformer
from sktime.transformations.series.summarize import WindowSummarizer
from sktime.forecasting.model_evaluation import evaluate
from sktime.split import SlidingWindowSplitter
from sktime.performance_metrics.forecasting import MeanAbsoluteError

from sktime.transformations.panel.compose import ColumnTransformer
from sktime.transformations.compose import ColumnEnsembleTransformer , TransformerPipeline
from sktime.pipeline import Pipeline, make_pipeline
# from sklearn.pipeline import Pipeline
from sktime.transformations.series.impute import Imputer
from sktime.transformations.series.adapt import TabularToSeriesAdaptor
from sklearn.preprocessing import LabelEncoder
from sktime.transformations.compose import Id
from sktime.performance_metrics.forecasting import MeanAbsolutePercentageError
from sktime.transformations.series.detrend import Deseasonalizer, Detrender
from sktime.transformations.series.boxcox import BoxCoxTransformer, LogTransformer
from sktime.transformations.series.cos import CosineTransformer
from sktime.transformations.series.exponent import ExponentTransformer, SqrtTransformer
from sktime.forecasting.compose import make_reduction
from sklearn.neighbors import KNeighborsRegressor
from sktime.utils.plotting import plot_series
from sktime.forecasting.arima import ARIMA
from sktime.forecasting.compose import TransformedTargetForecaster, ForecastingPipeline
from sktime.transformations.series.detrend import Deseasonalizer
from sktime.forecasting.compose import make_reduction
from sktime.transformations.series.exponent import ExponentTransformer
from lightgbm.sklearn import LGBMRegressor
from sktime.transformations.series.date import DateTimeFeatures
from sktime.transformations.series.boxcox import LogTransformer
from sktime.transformations.series.summarize import WindowSummarizer
from sktime.forecasting.model_evaluation import evaluate
from sktime.split import SlidingWindowSplitter
from sktime.performance_metrics.forecasting import MeanAbsoluteError, make_forecasting_scorer, MeanAbsolutePercentageError
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder
from sklearn.base import BaseEstimator, TransformerMixin


class DummyModel(BaseEstimator, TransformerMixin):
    def fit(self, X, y = None):
        print(f"shape: {X.shape}, X: {X}")
        return self 
    def predict(self, X):
        return [1]*len(X)

cv = SlidingWindowSplitter(fh=[1, 2], window_length=3, step_length=2)

numerical_columns = [
    "static_0"
]

categorical_columns = [
    "static_1"
]

window_kwargs = {
  "lag_feature": {
    "lag": [1, 4],
    # "mean": [[1, 8], [4, 12]],
    }

}

# Numerical Processing
numerical_preprocessing = TransformerPipeline(
    steps=[
        ("imputer", Imputer(method = "drift")),
    ]
)

# Categorical Processing
categorical_preprocessing = TransformerPipeline(steps = [
    ("imputer", Imputer(method = "drift")),
    ("encoder", TabularToSeriesAdaptor(OrdinalEncoder(handle_unknown = "use_encoded_value", unknown_value = -1))),
  #  ("imputer", TabularToSeriesAdaptor(SimpleImputer(strategy="most_frequent", keep_empty_features = True, missing_values = pd.NA))),
#    ("encoder", TabularToSeriesAdaptor(OneHotEncoder(handle_unknown = "ignore"))),
#    ("encoder", TabularToSeriesAdaptor(LabelEncoder())),
]
)

combine_preprocessing = ColumnEnsembleTransformer(
    transformers=[
        ("categorial", categorical_preprocessing, categorical_columns),
        ("numerical", numerical_preprocessing, numerical_columns),
        
        ("promotion_window", WindowSummarizer(**window_kwargs, target_cols=["static_1"], n_jobs = -1), ["static_1"]),
    ],
)



# # fh = ForecastingHorizon(X_test.index, is_relative=False)
# regressor = KNeighborsRegressor(n_neighbors=2)
# # regressor = LGBMRegressor(n_jobs = -1)
# forecaster = make_reduction(regressor, window_length=3, strategy="recursive")

target = TransformedTargetForecaster(steps=[
#   ("log1p", LogTransformer(offset = 1)),
  ("Forecaster", make_reduction(DummyModel(), window_length = 1)),
#   ("Forecaster", make_reduction(LGBMRegressor(n_jobs = -1, categorical_feature = [1], verbose = -1), window_length = 1))
  ])

pipe = ForecastingPipeline(steps = [
  # DateTimeFeatures(ts_freq="D"),
  # ("deseasonalize", Deseasonalizer(model="multiplicative", sp=3)),
  # ("Window_Summarizer", WindowSummarizer(target_cols=['static_0'])),
  ("combine_preprocessing", combine_preprocessing),
  ("target", target),
  # ("forecaster", forecaster)
])

# # results = evaluate(
# #     forecaster=pipe,
# #     X = X,
# #     y = y,
# #     cv = cv,
# #     scoring=[MeanAbsoluteError()]

# # )
# # results
pipe.fit(X = X_train, y = y_train)
y_pred = pipe.predict(fh=[1, 2, 3, 4], X=X_test)
print("y_pred: ", y_pred)


# plot_series(y_train.xs("id_0", level = "unique_id"),
#             y_test.xs("id_0", level = "unique_id"),
#             y_pred.xs("id_0", level = "unique_id"), 
#             labels=["y_train", "y_test", "y_pred"])

# numerical_preprocessing.fit_transform(X = X_train.loc[:, ["static_1"]])
# combine_preprocessing.fit_transform(X = X_train.loc[:, numerical_columns + categorical_columns])

# COMMAND ----------

len(y_train)

# COMMAND ----------

X_train["ds"]

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC date(billing_date) as billing_date,
# MAGIC dp_name,
# MAGIC ship_to,
# MAGIC banner,
# MAGIC sum(est_demand_cs) as est_demand,
# MAGIC sum(billing_cs) as billing_cs
# MAGIC from mt_predicted_order.mt_sale
# MAGIC where (trim(lower(banner)) = "saigon coop" or trim(lower(banner)) = "big_c") and year(billing_date) >= 2022
# MAGIC group by date(billing_date), dp_name, ship_to, banner

# COMMAND ----------

