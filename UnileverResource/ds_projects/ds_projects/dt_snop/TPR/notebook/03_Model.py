# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/ng-minh-hoang.dat@unilever.com/ds_projects/dt_snop/TPR/notebook/01_preprocessing_data"

# COMMAND ----------

!pip install --upgrade pip
!pip install catboost

# COMMAND ----------

df_backup = df_product_tpr.copy()

print(df_product_tpr.shape)
df_product_tpr.head(3)

# COMMAND ----------

df_product_tpr.iloc[:,:35].describe()

# COMMAND ----------

# MAGIC %md
# MAGIC # Utils Calculation

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## future trendline

# COMMAND ----------

from scipy.stats import linregress

def calculate_future_trendline(df, target_var, yearweek_cutoff = 202326):
    warnings.filterwarnings("ignore")
    df_result = pd.DataFrame(columns = ["BANNER","CATEGORY","DPNAME", "YEARWEEK", "TRENDLINE_DAILY"])

    for _, df_group in tqdm(df.groupby(["BANNER","CATEGORY","DPNAME"])):
        df_group = df_group[["BANNER","CATEGORY","DPNAME", "YEARWEEK", "DTWORKINGDAY", target_var]].sort_values("YEARWEEK").reset_index(drop = True)

        df_group["DAILY"] = np.where(
            df_group["DTWORKINGDAY"] == 0, 0, df_group[target_var] / df_group["DTWORKINGDAY"]
        )
    
        df_history = df_group[df_group["YEARWEEK"] <= yearweek_cutoff]
        df_group["DATE"] = pd.to_datetime(
            df_group["YEARWEEK"].astype(str) + "-1", format="%G%V-%w"
        )
        df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
        df_group["MONTH"] = df_group["DATE"].dt.month
        df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week

        df_group["COUNT WEEKS"] = (
            df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("count").astype(int)
        )
        df_group["WEEK ORDER"] = (
            df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("rank").astype(int)
        )

        df_group["YEARWEEK ORDER"] = df_group["YEARWEEK"].transform("rank").astype("int")
        df_group["TRENDLINE_DAILY"] = 0

        df_history = df_group[df_group["YEARWEEK"] <= yearweek_cutoff]
        df_future = df_group[df_group["YEARWEEK"] > yearweek_cutoff]
        try:
            fulltime_slope, fulltime_intercept, r_value, p_value, std_err = linregress(
                x=df_history["YEARWEEK ORDER"].values, y=df_history["DAILY"].values
            )

            last_26weeks_slope, last_26weeks_intercept, r_value, p_value, std_err = linregress(
                x=df_history["YEARWEEK ORDER"][-26:].values, y=df_history["DAILY"][-26:].values
            )

            weighted_arr = [1, 0.5]
            intercept_arr = [fulltime_intercept, last_26weeks_intercept]
            slope_arr = [fulltime_slope, last_26weeks_slope]

            forecast_intercept = sum(
                [intercept_arr[idx] * weight for idx, weight in enumerate(weighted_arr)]
            ) / sum(weighted_arr)
            forecast_slope = sum(
                [slope_arr[idx] * weight for idx, weight in enumerate(weighted_arr)]
            ) / sum(weighted_arr)

            df_future["TRENDLINE_DAILY"] = (
                forecast_intercept + forecast_slope * df_future["YEARWEEK ORDER"]
            )
            df_history["TRENDLINE_DAILY"] = fulltime_intercept + fulltime_slope * df_history["YEARWEEK ORDER"]
        except:
            pass

        df_temp = df_future.append(df_history)
        df_temp = df_temp[["BANNER","CATEGORY","DPNAME", "YEARWEEK", "TRENDLINE_DAILY"]]    
        df_result = pd.concat([df_result, df_temp])

    return df_result

# COMMAND ----------


df_trendline_feature = calculate_future_trendline(df_backup[df_backup["YEARWEEK"] >= 202152], "PRI_SALES_WITHOUT_BANDED", 202326)

df_trendline_feature["YEARWEEK"] = df_trendline_feature["YEARWEEK"].astype(int)
df_trendline_feature.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Phasing ratio

# COMMAND ----------

def calculate_phasing_ratio(key, df_group, target_var):
    warnings.filterwarnings("ignore")

    df_group = df_group[["KEY", "YEARWEEK", "WORKINGDAY", "FUTURE", target_var]]
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group[target_var] / df_group["WORKINGDAY"]
    )
    df_group = df_group.sort_values("YEARWEEK")
    df_group["DATE"] = pd.to_datetime(
        df_group["YEARWEEK"].astype(str) + "-1", format="%G%V-%w"
    )
    df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
    df_group["MONTH"] = df_group["DATE"].dt.month
    df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week

    df_group["COUNT WEEKS"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("count").astype(int)
    )
    df_group["WEEK ORDER"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("rank").astype(int)
    )

    df_history = df_group.query("FUTURE == 'N' ")
    df_history["RATIO WEEK/MONTH"] = df_history["DAILY"] / df_history.groupby(
        ["YEAR", "MONTH"]
    )["DAILY"].transform(sum)

    df_ratio_generic = (
        df_history.groupby(["COUNT WEEKS", "WEEK ORDER"])["RATIO WEEK/MONTH"]
        .mean()
        .reset_index()
    )
    df_ratio_generic = df_ratio_generic.query(" `COUNT WEEKS` >= 4")
    df_ratio_generic = df_ratio_generic.rename(
        columns={"RATIO WEEK/MONTH": "RATIO_GENERIC"}
    )

    df_group = df_group.merge(df_ratio_generic, on=["COUNT WEEKS", "WEEK ORDER"])

    df_group["KEY"] = key
    df_group = df_group[["KEY", "YEARWEEK", "RATIO_GENERIC"]]

    return df_group

# COMMAND ----------

def create_features_phasing_ratio(df, target_var):
    ratio_WEEK_MONTH_arr = []

    for key, df_group in tqdm(
        df.groupby(["BANNER", "CATEGORY", "DPNAME"])
    ):
        df_group = df_group.sort_values("DATE")
        df_group[target_var] = df_group[target_var].fillna(0)

        df_group["DAILY_SALES"] = np.where(
            df_group["DTWORKINGDAY"] == 0, 0, df_group[target_var] / df_group["DTWORKINGDAY"]
        )

        df_group["WEEK/MONTH RATIO"] = df_group["DAILY_SALES"] / df_group.groupby(
            ["YEAR", "MONTH"]
        )["DAILY_SALES"].transform("sum")

        df_group["WEEK/MONTH COUNT"] = df_group.groupby(["YEAR", "MONTH"])[
            "YEARWEEK"
        ].transform("count")

        df_group["WEEK/MONTH ORDER"] = df_group.groupby(["YEAR", "MONTH"])[
            "YEARWEEK"
        ].transform("rank")

        ratio_WEEK_MONTH = (
            df_group.groupby(["WEEK/MONTH COUNT", "WEEK/MONTH ORDER"])
            .agg({"WEEK/MONTH RATIO": ["mean", "median", "std"]})
            .reset_index()
        )

        ratio_WEEK_MONTH.columns = ["_".join(col) for col in ratio_WEEK_MONTH.columns]
        ratio_WEEK_MONTH["KEY"] = "|".join(key)
        ratio_WEEK_MONTH_arr.append(ratio_WEEK_MONTH)

    # ********************************************************
    df_ratio_WEEK_MONTH = pd.concat(ratio_WEEK_MONTH_arr)
    df_ratio_WEEK_MONTH = df_ratio_WEEK_MONTH.query("`WEEK/MONTH COUNT_` >= 4")
    df_ratio_WEEK_MONTH = df_ratio_WEEK_MONTH.dropna(
        subset=["WEEK/MONTH RATIO_median"]
    )
    df_ratio_WEEK_MONTH["KEY"] = df_ratio_WEEK_MONTH["KEY"].str.replace(
        "NATIONWIDE\|NATIONWIDE\|", ""
    )

    df_ratio_WEEK_MONTH["WEEK/MONTH ORDER_"] = df_ratio_WEEK_MONTH[
        "WEEK/MONTH ORDER_"
    ].astype(int)

    df_ratio_WEEK_MONTH.columns = [
        "WEEK_MONTH_COUNT",
        "WEEK_MONTH_ORDER",
        "RATIO_MEAN",
        "RATIO_MEDIAN",
        "RATIO_STD",
        "KEY",
    ]

    return df_ratio_WEEK_MONTH

# COMMAND ----------

df_phasing_features = df_product_tpr[
    [
        "BANNER",
        "CATEGORY",
        "DPNAME",
        "YEARWEEK",
        "PRI_SALES",
        "PRI_SALES_WITHOUT_BANDED",
        "SEC_SALES",
        "DATE",
        "DTWORKINGDAY",
        "YEAR",
        "MONTH",
        "QUARTER",
    ]
].copy()
df_phasing_features = df_phasing_features[df_phasing_features["YEARWEEK"] >= 202152]
df_phasing_features["KEY"] = (
    "NATIONWIDE|" + df_phasing_features["CATEGORY"] + "|" + df_phasing_features["DPNAME"]
)

df_phasing_features["WEEK_MONTH_COUNT"] = (
    df_phasing_features.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("count")
    .astype(int)
)
df_phasing_features["WEEK_MONTH_ORDER"] = (
    df_phasing_features.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("rank")
    .astype(int)
)

df_phasing_features = df_phasing_features.sort_values(["KEY", "YEARWEEK"]).reset_index(
    drop=True
)

df_phasing_features["FUTURE"] = np.where(df_phasing_features["YEARWEEK"] <= 202326, "N", "Y")

# COMMAND ----------

df_ratio_WEEK_MONTH = create_features_phasing_ratio(df_phasing_features.query("FUTURE == 'N'"), "PRI_SALES_WITHOUT_BANDED")
df_ratio_WEEK_MONTH = df_ratio_WEEK_MONTH.fillna(0)
df_phasing_features = df_phasing_features.merge(df_ratio_WEEK_MONTH, on = ["WEEK_MONTH_COUNT","WEEK_MONTH_ORDER","KEY"], how = "left")

df_phasing_features = df_phasing_features.rename(columns = {"WEEK_MONTH_ORDER" : "WEEK_OF_MONTH"})

# COMMAND ----------

df_phasing_features.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Timeseries features

# COMMAND ----------

# MAGIC %md
# MAGIC ### TS Target

# COMMAND ----------

from statsmodels.graphics.tsaplots import plot_pacf, plot_acf

df_agg_acf = df_product_tpr[(df_product_tpr["YEARWEEK"].between(202001, 202352))].groupby(["YEARWEEK"])["PRI_SALES_WITHOUT_BANDED"].sum().reset_index()

plot_acf(df_agg_acf["PRI_SALES_WITHOUT_BANDED"], lags = 26, title = "ACF of total")
plot_pacf(df_agg_acf["PRI_SALES_WITHOUT_BANDED"], lags = 26, title = "PACF of total")

# COMMAND ----------

df_agg_acf = df_product_tpr[(df_product_tpr["YEARWEEK"].between(202152, 202352))].groupby(["CATEGORY","YEARWEEK"])["PRI_SALES_WITHOUT_BANDED"].sum().reset_index()

for key, df_group in df_agg_acf.groupby("CATEGORY"):
    df_group = df_group.sort_values("YEARWEEK").reset_index(drop = True)
    plot_acf(df_group["PRI_SALES_WITHOUT_BANDED"], lags = 12, title = 'ACF of ' + str(key))
    plot_pacf(df_group["PRI_SALES_WITHOUT_BANDED"], lags = 12, title = 'PACF of ' + str(key))

# COMMAND ----------

df_ts_feature_total = df_product_tpr[["CATEGORY","DATE","DPNAME","PRI_SALES_WITHOUT_BANDED"]].copy()

df_explode_date = pd.DataFrame(
    data={
        "DPNAME": df_product_tpr["DPNAME"].unique(),
        "DATE_RANGE": [
            pd.date_range(
                start=df_product_tpr[df_product_tpr["DPNAME"] == dpname]["DATE"].min(),
                end=df_product_tpr[df_product_tpr["DPNAME"] == dpname]["DATE"].max(),
                freq="W-MON",
            )
            for dpname in df_product_tpr["DPNAME"].unique()
        ],
    }
)
df_explode_date = df_explode_date.explode("DATE_RANGE")
df_explode_date.columns = ["DPNAME","DATE"]

# COMMAND ----------

df_ts_feature_total = df_ts_feature_total.merge(df_explode_date, on =["DPNAME", "DATE"], how = "right")
df_ts_feature_total = df_ts_feature_total.fillna(0)

# COMMAND ----------

# Lag
threshold_yw_cutoff = 26

for lag_number in [
    threshold_yw_cutoff,
    threshold_yw_cutoff + 1,
    threshold_yw_cutoff + 2,
    threshold_yw_cutoff + 3,
    threshold_yw_cutoff + 4,
    threshold_yw_cutoff + 5,
    threshold_yw_cutoff + 12,
]:
    df_ts_feature_total[f"total_lag_{lag_number}"] = df_ts_feature_total.groupby(["CATEGORY", "DPNAME"])[
        "PRI_SALES_WITHOUT_BANDED"
    ].shift(lag_number)

# lag then moving
# Moving avg, median, min, max
for lag_number in [threshold_yw_cutoff, threshold_yw_cutoff + 3]:
    for moving_number in [2, 3, 4, 8, 13]:
        df_ts_feature_total[
            f"total_lag_{lag_number}_moving_mean_{moving_number}"
        ] = df_ts_feature_total.groupby(["CATEGORY", "DPNAME"])[
            "PRI_SALES_WITHOUT_BANDED"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).mean()
        )
        df_ts_feature_total[
            f"total_lag_{lag_number}_moving_std_{moving_number}"
        ] = df_ts_feature_total.groupby(["CATEGORY", "DPNAME"])[
            "PRI_SALES_WITHOUT_BANDED"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).std()
        )
        df_ts_feature_total[
            f"total_lag_{lag_number}_moving_median_{moving_number}"
        ] = df_ts_feature_total.groupby(["CATEGORY", "DPNAME"])[
            "PRI_SALES_WITHOUT_BANDED"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).median()
        )
        df_ts_feature_total[
            f"total_lag_{lag_number}_moving_min_{moving_number}"
        ] = df_ts_feature_total.groupby(["CATEGORY", "DPNAME"])[
            "PRI_SALES_WITHOUT_BANDED"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).min()
        )
        df_ts_feature_total[
            f"total_lag_{lag_number}_moving_max_{moving_number}"
        ] = df_ts_feature_total.groupby(["CATEGORY", "DPNAME"])[
            "PRI_SALES_WITHOUT_BANDED"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).max()
        )

df_ts_feature_total = df_ts_feature_total.fillna(0)
df_ts_feature_total.shape

# COMMAND ----------

display(df_ts_feature_total[df_ts_feature_total["DPNAME"] == "OMO RED 6000 GR"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Ts Baseline

# COMMAND ----------

from statsmodels.graphics.tsaplots import plot_pacf, plot_acf

df_agg_acf = df_product_tpr[(df_product_tpr["YEARWEEK"].between(202001, 202352))].groupby(["YEARWEEK"])["PRI_BASELINE"].sum().reset_index()

plot_acf(df_agg_acf["PRI_BASELINE"], lags = 26, title = "ACF of total")
plot_pacf(df_agg_acf["PRI_BASELINE"], lags = 26, title = "PACF of total")

# COMMAND ----------

df_ts_feature_baseline = df_product_tpr[["CATEGORY","DATE","DPNAME","PRI_BASELINE"]].copy()

df_explode_date = pd.DataFrame(
    data={
        "DPNAME": df_product_tpr["DPNAME"].unique(),
        "DATE_RANGE": [
            pd.date_range(
                start=df_product_tpr[df_product_tpr["DPNAME"] == dpname]["DATE"].min(),
                end=df_product_tpr[df_product_tpr["DPNAME"] == dpname]["DATE"].max(),
                freq="W-MON",
            )
            for dpname in df_product_tpr["DPNAME"].unique()
        ],
    }
)
df_explode_date = df_explode_date.explode("DATE_RANGE")
df_explode_date.columns = ["DPNAME","DATE"]

# COMMAND ----------

df_ts_feature_baseline = df_ts_feature_baseline.merge(df_explode_date, on =["DPNAME", "DATE"], how = "right")
df_ts_feature_baseline = df_ts_feature_baseline.fillna(0)

# COMMAND ----------

# Lag
threshold_yw_cutoff = 1

for lag_number in [
    threshold_yw_cutoff,
    threshold_yw_cutoff + 1,
    threshold_yw_cutoff + 2,
    threshold_yw_cutoff + 3,
    threshold_yw_cutoff + 4,
    threshold_yw_cutoff + 5,
    threshold_yw_cutoff + 12,
]:
    df_ts_feature_baseline[f"baseline_lag_{lag_number}"] = df_ts_feature_baseline.groupby(["CATEGORY", "DPNAME"])[
        "PRI_BASELINE"
    ].shift(lag_number)

# lag then moving
# Moving avg, median, min, max
for lag_number in [threshold_yw_cutoff, threshold_yw_cutoff + 3]:
    for moving_number in [2, 3, 4, 8, 13]:
        df_ts_feature_baseline[
            f"baseline_lag_{lag_number}_moving_mean_{moving_number}"
        ] = df_ts_feature_baseline.groupby(["CATEGORY", "DPNAME"])[
            "PRI_BASELINE"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).mean()
        )
        df_ts_feature_baseline[
            f"baseline_lag_{lag_number}_moving_std_{moving_number}"
        ] = df_ts_feature_baseline.groupby(["CATEGORY", "DPNAME"])[
            "PRI_BASELINE"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).std()
        )
        df_ts_feature_baseline[
            f"baseline_lag_{lag_number}_moving_median_{moving_number}"
        ] = df_ts_feature_baseline.groupby(["CATEGORY", "DPNAME"])[
            "PRI_BASELINE"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).median()
        )
        df_ts_feature_baseline[
            f"baseline_lag_{lag_number}_moving_min_{moving_number}"
        ] = df_ts_feature_baseline.groupby(["CATEGORY", "DPNAME"])[
            "PRI_BASELINE"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).min()
        )
        df_ts_feature_baseline[
            f"baseline_lag_{lag_number}_moving_max_{moving_number}"
        ] = df_ts_feature_baseline.groupby(["CATEGORY", "DPNAME"])[
            "PRI_BASELINE"
        ].apply(
            lambda x: x.shift(lag_number).rolling(window=moving_number).max()
        )

df_ts_feature_baseline = df_ts_feature_baseline.fillna(0)
df_ts_feature_baseline.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Regression

# COMMAND ----------

from sklearn import tree
from sklearn.tree import DecisionTreeRegressor, export_graphviz
from sklearn.ensemble import RandomForestRegressor, AdaBoostRegressor, ExtraTreesRegressor
from xgboost import XGBRegressor, plot_tree
from catboost import CatBoostRegressor
from lightgbm import LGBMRegressor

from sklearn.metrics import (
    mean_absolute_error as MAE,
    mean_squared_error as MSE,
    mean_absolute_percentage_error as MAPE,
    make_scorer
)

from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV, KFold
from sklearn.feature_selection import RFE
from sklearn.inspection import permutation_importance
import shap

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr):
    df_group[actual_col] = df_group[actual_col].fillna(0)

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"CATEGORY": key, "Sum_actual": sum_actualsale}
    performance["Mean_actual"] = df_group[actual_col].mean()
    performance["Std_actual"] = df_group[actual_col].std()

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col].fillna(0)
        df_group[predict_col] = df_group[predict_col].replace([-np.inf, np.inf], 0)

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()

        performance["Sum_predict_" + predict_col] = df_group[predict_col].sum()
        performance["Mean_predict_" + predict_col] = df_group[predict_col].mean()
        performance["FA_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error
        performance["MAE_" + predict_col] = MAE(
            df_group[actual_col], df_group[predict_col]
        )
        performance["RMSE_" + predict_col] = MSE(
            df_group[actual_col], df_group[predict_col], squared=False
        )

    return performance


def performance_check(key, df_group, actual_col, predict_col):
    df_group = df_group.groupby("YEARWEEK")[[actual_col, predict_col]].sum().reset_index()
    df_group["PFM"] = abs((df_group[actual_col] - df_group[predict_col]) / df_group[actual_col])
    performance = {
        "Layer": key,
        "Performance": df_group["PFM"].mean(),
        "mean_predict": df_group[predict_col].mean(),
        "median_predict": df_group[predict_col].median(),
    }
    return performance

# COMMAND ----------

df_model = df_product_tpr.copy().drop(
    ["SEC_SALES", "SEC_BASELINE_ORG", "BANNER", "REGION"], axis=1
)

df_model = df_model.merge(
    df_ts_feature_total,
    on=["CATEGORY", "DPNAME", "DATE", "PRI_SALES_WITHOUT_BANDED"],
    how="left",
)

df_model = df_model.merge(
    df_trendline_feature[["CATEGORY", "DPNAME", "YEARWEEK", "TRENDLINE_DAILY"]],
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)
# df_model["TRENDLINE_DAILY"] = df_model["TRENDLINE_DAILY"] * df_model["DTWORKINGDAY"] # weekly

df_model = df_model.merge(
    df_phasing_features[
        [
            "CATEGORY",
            "DPNAME",
            "YEARWEEK",
            "RATIO_MEAN",
            "RATIO_MEDIAN",
            "RATIO_STD",
        ]
    ],
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)

df_model["ABS_UPLIFT"] = df_model["PRI_SALES_WITHOUT_BANDED"] - df_model["PRI_BASELINE"]
df_model["PCT_UPLIFT"] = np.where(
    df_model["PRI_BASELINE"] == 0, 0, df_model["ABS_UPLIFT"] / df_model["PRI_BASELINE"]
)

df_model["CLASSIFY_UPLIFT"] = np.where(
    df_model["ABS_UPLIFT"] >= 0, 1, 0
)  # positive = 1, negative = 0

df_model = df_model[
    ((df_model["PCT_UPLIFT"].between(-1, 1)) & (df_model["YEARWEEK"] <= 202326))
    | (df_model["YEARWEEK"] > 202326)
]
# df_model["ABS_UPLIFT"] = np.where(
#     df_model["PCT_UPLIFT"].between(-0.5, 0.5),
#     df_model["ABS_UPLIFT"],
#     df_model["PRI_BASELINE"] * 0.5,
# )

df_model = df_model.sort_values(["CATEGORY", "DPNAME", "YEARWEEK"]).reset_index(
    drop=True
)

# COMMAND ----------

# train from end 12/2021 to end 6/2023
X_train = df_model[df_model["YEARWEEK"].between(202152, 202326)].drop(
    [
        "DATE",
        "PRI_SALES",
        "PRI_SALES_WITHOUT_BANDED",
        "ABS_UPLIFT",
        "PCT_UPLIFT",
        "CLASSIFY_UPLIFT",
    ],
    axis=1,
)
# predict total sales use PRI_SALES_WITHOUT_BANDED as label
# predict absolute uplift TPR use ABS_UPLIFT as label
Y_train = df_model[df_model["YEARWEEK"].between(202152, 202326)]["PRI_SALES_WITHOUT_BANDED"]

X_test = df_model[df_model["YEARWEEK"] > 202326].drop(
    [
        "DATE",
        "PRI_SALES",
        "PRI_SALES_WITHOUT_BANDED",
        "ABS_UPLIFT",
        "PCT_UPLIFT",
        "CLASSIFY_UPLIFT",
    ],
    axis=1,
)
Y_test = df_model[df_model["YEARWEEK"] > 202326]["PRI_SALES_WITHOUT_BANDED"]

X_train.shape, Y_train.shape, X_test.shape, Y_test.shape

# COMMAND ----------

categorical_cols = X_train.select_dtypes(include=[object]).columns.values
print(categorical_cols)

X_train[X_train.columns.difference(categorical_cols)] = X_train[X_train.columns.difference(categorical_cols)].fillna(0)
X_test[X_test.columns.difference(categorical_cols)] = X_test[X_test.columns.difference(categorical_cols)].fillna(0)

X_train[categorical_cols] =X_train[categorical_cols].astype("category")
X_test[categorical_cols] =X_test[categorical_cols].astype("category") 
# X_train = X_train.drop(categorical_cols, axis = 1)
# X_test = X_test.drop(categorical_cols, axis = 1)

# COMMAND ----------

model = LGBMRegressor()
# model = XGBRegressor(tree_method = "hist",enable_categorical = True)
model.fit(X_train, Y_train)
model.get_params()

# COMMAND ----------

# make_score_tuning = make_scorer(MSE,greater_is_better=False)

# def tune_model(X, y):
#     pipe = Pipeline(
#         steps=[("tree", LGBMRegressor(max_depth=-1, n_jobs=-1))],
#         verbose=True,
#     )
#     param_grid = {
#         # "tree__max_depth" : [-1, 4, np.arange(8, 65, 8)],
#         "tree__boosting_type": ["gbdt", "dart"],
#         "tree__n_estimators": [50, 100, 200],
#         "tree__learning_rate": [0.1, 1e-3, 1e-6],
#         "tree__reg_alpha": [0, 0.1, 0.5, 1],
#         "tree__reg_lamda": [0, 0.5],
#     }
#     model = GridSearchCV(pipe, param_grid, scoring=make_score_tuning, n_jobs=-1)
#     model.fit(X, y)

#     return model

# model = tune_model(X_train, Y_train)

# print(model.best_score_)
# print(model.best_params_)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### overall

# COMMAND ----------

y_pred = model.predict(X_test)
df_evaluate = X_test.copy()

# absolute uplift TPR label
# df_evaluate["ACTUAL_UPLIFT"] = Y_test 
# df_evaluate["PREDICTED_UPLIFT"] = y_pred
# df_evaluate["ACTUAL_TOTAL"] = df_evaluate["ACTUAL_UPLIFT"] + df_evaluate["PRI_BASELINE"]
# df_evaluate["PREDICTED_TOTAL"] = df_evaluate["PREDICTED_UPLIFT"] + df_evaluate["PRI_BASELINE"]

# Total sales label
df_evaluate["ACTUAL_TOTAL"] = Y_test 
df_evaluate["PREDICTED_TOTAL"] = y_pred
df_evaluate["ACTUAL_UPLIFT"] = df_evaluate["ACTUAL_TOTAL"] - df_evaluate["PRI_BASELINE"]
df_evaluate["PREDICTED_UPLIFT"] = df_evaluate["PREDICTED_TOTAL"] - df_evaluate["PRI_BASELINE"]

df_evaluate["PRI_SALES"] = df_model[df_model["YEARWEEK"] > 202326]["PRI_SALES"]

# df_dp_zero_sales = df_evaluate.groupby("DPNAME")["ACTUAL_TOTAL"].sum().reset_index()
# df_dp_zero_sales = df_dp_zero_sales[df_dp_zero_sales["ACTUAL_TOTAL"] == 0]

print(performance_check("total", df_evaluate, "ACTUAL_UPLIFT", "PREDICTED_UPLIFT"))
print(performance_check("total", df_evaluate, "ACTUAL_TOTAL", "PREDICTED_TOTAL"))
print(performance_check("total", df_evaluate, "ACTUAL_TOTAL", "PRI_BASELINE"))

# COMMAND ----------

accuracy_check("total", df_evaluate, "ACTUAL_TOTAL", ["PREDICTED_TOTAL","PRI_BASELINE"])

# COMMAND ----------

df_evaluate[["CATEGORY","UOM_PRODUCT","DPNAME","YEARWEEK","YEAR","MONTH","PRI_SALES","BANDED_COUNT","PROMOTION_COUNT","ACTUAL_TOTAL","PREDICTED_TOTAL","PRI_BASELINE","ACTUAL_UPLIFT","PREDICTED_UPLIFT"]].to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/df_eval_tpr.csv")

# COMMAND ----------

df_agg_total = df_evaluate.groupby("YEARWEEK")[["ACTUAL_TOTAL", "PREDICTED_TOTAL","PRI_BASELINE","ACTUAL_UPLIFT","PREDICTED_UPLIFT"]].sum().reset_index()
df_agg_total["YEARWEEK"] = df_agg_total["YEARWEEK"].astype(str)
print(performance_check("total", df_agg_total, "ACTUAL_TOTAL", "PREDICTED_TOTAL"))
px.line(df_agg_total, x = "YEARWEEK", y = ["ACTUAL_TOTAL", "PREDICTED_TOTAL","PRI_BASELINE","ACTUAL_UPLIFT","PREDICTED_UPLIFT"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cate, bin

# COMMAND ----------

pd.DataFrame.from_dict(
        [
            performance_check(key, df_group, "ACTUAL_UPLIFT", "PREDICTED_UPLIFT")
            for key, df_group in  df_evaluate.groupby("CATEGORY")
        ]
)

# COMMAND ----------

display(pd.DataFrame.from_dict(
        [
            performance_check(key, df_group, "ACTUAL_TOTAL", "PREDICTED_TOTAL")
            for key, df_group in  df_evaluate.groupby("CATEGORY")
        ]
))
display(pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, "ACTUAL_TOTAL", ["PREDICTED_TOTAL","PRI_BASELINE"])
            for key, df_group in  df_evaluate.groupby("CATEGORY")
        ]
))

# COMMAND ----------

# df_evaluate["PCT_UPLIFT"] = np.where(df_evaluate["PRI_BASELINE_WEEKLY"] == 0,0,df_evaluate["ACTUAL_UPLIFT"] / df_evaluate["PRI_BASELINE_WEEKLY"])

df_evaluate["BIN_UPLIFT_qcut"] = pd.qcut(
    df_evaluate["ACTUAL_UPLIFT"], q=10, duplicates = "drop"
)
print(df_evaluate["BIN_UPLIFT_qcut"].value_counts(normalize = True).sort_index())
pd.DataFrame.from_dict(
        [
            performance_check(key, df_group, "ACTUAL_UPLIFT", "PREDICTED_UPLIFT")
            for key, df_group in  df_evaluate.groupby("BIN_UPLIFT_qcut")
        ]
).sort_values("Layer")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### something

# COMMAND ----------

f_importance = pd.DataFrame({'Feature_Name': X_train.columns, 'Weight_Importance': model.feature_importances_})

f_importance_sorted = f_importance.sort_values(by='Weight_Importance', ascending=False).reset_index(drop=True)
display(f_importance_sorted)

# COMMAND ----------

display(df_model[df_model["YEARWEEK"].between(202201, 2022352)].corr()[["PRI_SALES_WITHOUT_BANDED","ABS_UPLIFT","PCT_UPLIFT"]].reset_index())

# COMMAND ----------

df_temp = df_model[df_model["YEARWEEK"].between(202326, 202352)].copy()
df_temp[["PCT_NEG_UPLIFT","PCT_POS_UPLIFT"]] = 0
df_temp["PCT_NEG_UPLIFT"][df_temp["ABS_UPLIFT"] < 0] = 1 / df_temp.shape[0]
df_temp["PCT_POS_UPLIFT"][df_temp["ABS_UPLIFT"] >= 0] = 1 / df_temp.shape[0]

df_agg_total = (
    df_temp.groupby(["CATEGORY","YEARWEEK"])[
        [
            "PRI_SALES_WITHOUT_BANDED",
            "PRI_BASELINE",
            "ABS_UPLIFT",
            "PCT_UPLIFT",
            "PCT_NEG_UPLIFT",
            "PCT_POS_UPLIFT",
            "RATIO_MEDIAN",
            "RATIO_MEAN",
            "RATIO_STD",
            "TRENDLINE_DAILY",
            "BENEFIT_TPR_sum_VALUE",
            "BENEFIT_TPR_sum_U GIFT",
            "PROMOTION_COUNT",
        ]
    ]
    .sum()
    .reset_index()
)
df_agg_total["DATE"] = pd.to_datetime(df_agg_total["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")

# COMMAND ----------

df_agg_total["BIN"] = pd.qcut(df_agg_total["ABS_UPLIFT"], q = 10)
df_agg_total.groupby(["BIN"])[["PRI_SALES_WITHOUT_BANDED","PRI_BASELINE","PCT_NEG_UPLIFT","PCT_POS_UPLIFT"]].sum().sort_index()

# consider pos uplift only,

# COMMAND ----------

df_agg_total["BIN"] = pd.qcut(df_agg_total["RATIO_MEAN"], q = 10)
df_agg_total.groupby(["BIN"])[["PRI_SALES_WITHOUT_BANDED","PRI_BASELINE","PCT_NEG_UPLIFT","PCT_POS_UPLIFT"]].sum().sort_index()

# COMMAND ----------

df_agg_total["BIN"] = pd.qcut(df_agg_total["TRENDLINE_DAILY"], q = 10)
df_agg_total.groupby(["BIN"])[["PRI_SALES_WITHOUT_BANDED","PRI_BASELINE","PCT_NEG_UPLIFT","PCT_POS_UPLIFT"]].sum().sort_index()

# COMMAND ----------

df_agg_total["BIN"] = pd.qcut(df_agg_total["PROMOTION_COUNT"], q = 10)
df_agg_total.groupby(["BIN"])[["PRI_SALES_WITHOUT_BANDED","PRI_BASELINE","PCT_NEG_UPLIFT","PCT_POS_UPLIFT"]].sum().sort_index()

# COMMAND ----------

for dpname in [
    "OMO LIQUID MATIC CFT SS (POU) 3.7KG",
    "OMO RED 6000 GR",
    "SUNLIGHT LEMON 3600G",
]:
    temp_plot = df_evaluate[(df_evaluate["DPNAME"] == dpname)]
    print("error",(temp_plot["ACTUAL_TOTAL"] - temp_plot["PREDICTED_TOTAL"]).abs().sum())
    print("FA",1- ( temp_plot["ACTUAL_TOTAL"] - temp_plot["PREDICTED_TOTAL"]).abs().sum() / temp_plot["ACTUAL_TOTAL"].sum())
    fig = go.Figure(
        data=[
            go.Scatter(
                x=temp_plot["YEARWEEK"],
                y=temp_plot["ACTUAL_UPLIFT"],
                name="actual uplift",
                marker={"color": "green"},
            ),
            go.Scatter(
                x=temp_plot["YEARWEEK"],
                y=temp_plot["PREDICTED_UPLIFT"],
                name="predicted uplift",
                connectgaps=False,
                marker={"color": "red"},
                mode="lines+markers",
            ),
            # go.Scatter(
            #     x=temp_plot["YEARWEEK"],
            #     y=temp_plot["PRI_BASELINE_WEEKLY"],
            #     name="baseline primary",
            #     connectgaps=False,
            #     marker={"color": "purple"},
            #     mode="lines+markers",
            # ),
        ]
    )
    # fig.add_trace(go.Scatter(
    #     x=[0, 1, 2],
    #     y=[3, 3, 3],
    #     mode="lines+text",
    #     name="Lines and Text",
    #     text=["Text G", "Text H", "Text I"],
    #     textposition="bottom center"
    # ))
    fig.update_layout(title=dpname, hovermode= "x unified")

    fig.show()

# COMMAND ----------

# feature selection
rfe = RFE(LGBMRegressor())
rfe_features = rfe.fit(X_train.drop(categorical_cols, axis = 1), Y_train)

df_rfe = pd.DataFrame(
    data={
        "Num_features": rfe_features.n_features_,
        "Feature_name": X_train.drop(categorical_cols, axis = 1).columns,
        "Selected_features": rfe_features.support_,
        "Feature_ranking": rfe_features.ranking_,
    }   
)
display(df_rfe)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Classification

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Phasing ratio uplift contribute baseline

# COMMAND ----------

def uplift_phasing_ratio(df, target_var):
    ratio_WEEK_MONTH_arr = []

    for key, df_group in tqdm(
        df.groupby(["BANNER", "CATEGORY", "DPNAME"])
    ):
        df_group = df_group.sort_values("DATE")
        df_group[target_var] = df_group[target_var].fillna(0)

        df_group["WEEK/MONTH RATIO"] = np.where(
            df_group["DTWORKINGDAY"] == 0, 0, df_group[target_var] / df_group["DTWORKINGDAY"]
        ) 

        df_group["WEEK/MONTH COUNT"] = df_group.groupby(["YEAR", "MONTH"])[
            "YEARWEEK"
        ].transform("count")

        df_group["WEEK/MONTH ORDER"] = df_group.groupby(["YEAR", "MONTH"])[
            "YEARWEEK"
        ].transform("rank")

        ratio_WEEK_MONTH = (
            df_group.groupby(["WEEK/MONTH COUNT", "WEEK/MONTH ORDER"])
            .agg({"WEEK/MONTH RATIO": ["mean", "median", "std"]})
            .reset_index()
        )

        ratio_WEEK_MONTH.columns = ["_".join(col) for col in ratio_WEEK_MONTH.columns]
        ratio_WEEK_MONTH["KEY"] = "|".join(key)
        ratio_WEEK_MONTH_arr.append(ratio_WEEK_MONTH)

    # ********************************************************
    df_ratio_WEEK_MONTH = pd.concat(ratio_WEEK_MONTH_arr)
    df_ratio_WEEK_MONTH = df_ratio_WEEK_MONTH.query("`WEEK/MONTH COUNT_` >= 4")
    df_ratio_WEEK_MONTH = df_ratio_WEEK_MONTH.dropna(
        subset=["WEEK/MONTH RATIO_median"]
    )
    df_ratio_WEEK_MONTH["KEY"] = df_ratio_WEEK_MONTH["KEY"].str.replace(
        "NATIONWIDE\|NATIONWIDE\|", ""
    )

    df_ratio_WEEK_MONTH["WEEK/MONTH ORDER_"] = df_ratio_WEEK_MONTH[
        "WEEK/MONTH ORDER_"
    ].astype(int)

    df_ratio_WEEK_MONTH.columns = [
        "WEEK_MONTH_COUNT",
        "WEEK_MONTH_ORDER",
        "RATIO_MEAN_UPLIFT",
        "RATIO_MEDIAN_UPLIFT",
        "RATIO_STD_UPLIFT",
        "KEY",
    ]

    return df_ratio_WEEK_MONTH

# COMMAND ----------


df_uplift_phasing = df_product_tpr[
    [
        "BANNER",
        "CATEGORY",
        "DPNAME",
        "YEARWEEK",
        "PRI_SALES",
        "PRI_SALES_WITHOUT_BANDED",
        "PRI_BASELINE",
        "DATE",
        "DTWORKINGDAY",
        "YEAR",
        "MONTH",
        "QUARTER",
    ]
].copy()
df_uplift_phasing = df_uplift_phasing[df_uplift_phasing["YEARWEEK"] >= 202152]
df_uplift_phasing["KEY"] = (
    "NATIONWIDE|" + df_uplift_phasing["CATEGORY"] + "|" + df_uplift_phasing["DPNAME"]
)
df_uplift_phasing["PCT_UPLIFT"] = np.where(df_uplift_phasing["PRI_BASELINE"] == 0, 0 , (df_uplift_phasing["PRI_SALES_WITHOUT_BANDED"] - df_uplift_phasing["PRI_BASELINE"]) / df_uplift_phasing["PRI_BASELINE"])

df_uplift_phasing["WEEK_MONTH_COUNT"] = (
    df_uplift_phasing.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("count")
    .astype(int)
)
df_uplift_phasing["WEEK_MONTH_ORDER"] = (
    df_uplift_phasing.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("rank")
    .astype(int)
)

df_uplift_phasing = df_uplift_phasing.sort_values(["KEY", "YEARWEEK"]).reset_index(
    drop=True
)

df_uplift_phasing["FUTURE"] = np.where(df_uplift_phasing["YEARWEEK"] <= 202326, "N", "Y")

# COMMAND ----------

df_ratio_WEEK_MONTH = uplift_phasing_ratio(
    df_uplift_phasing[
        (df_uplift_phasing["FUTURE"] == "N")
        # & (df_uplift_phasing["PCT_UPLIFT"].between(-1, 0))
    ],
    "PCT_UPLIFT",
)
df_ratio_WEEK_MONTH = df_ratio_WEEK_MONTH.fillna(0)
df_uplift_phasing = df_uplift_phasing.merge(
    df_ratio_WEEK_MONTH, on=["WEEK_MONTH_COUNT", "WEEK_MONTH_ORDER", "KEY"], how="left"
)

df_uplift_phasing = df_uplift_phasing.rename(
    columns={"WEEK_MONTH_ORDER": "WEEK_OF_MONTH"}
)

# COMMAND ----------

df_uplift_phasing.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Classification model

# COMMAND ----------

from sklearn import tree
from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from xgboost import XGBClassifier, plot_tree
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier

from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV, KFold
from sklearn.feature_selection import RFE
# from sklearn.inspection import permutation_importance

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, \
                        confusion_matrix, classification_report, ConfusionMatrixDisplay

def total_pfm_classfication(y_true, y_pred):
    print("accuracy", accuracy_score(y_true, y_pred))
    print("precision", precision_score(y_true, y_pred, average = 'weighted', pos_label= 1))
    print("recall", recall_score(y_true, y_pred, average = 'weighted', pos_label= 1))
    print("F1", f1_score(y_true, y_pred, average = 'weighted'))
    # print("ROC_AUC ", roc_auc_score(y_true, y_pred, average = 'weighted'))

    report = classification_report(y_true, y_pred)
    print("Classification Report\n",report)

    cm = confusion_matrix(y_true,y_pred)
	# tn, fp, fn, tp = confusion_matrix(test_labels, y_pred).ravel()
    display_label = np.unique(y_true)
    cm_display = ConfusionMatrixDisplay(confusion_matrix = cm, display_labels = display_label)
    cm_display.plot()
    plt.show()

def classify_check(key, df_group, actual_col, predict_col):
    pfm = abs(
        (df_group[actual_col] - df_group[predict_col]).sum()
        / df_group[actual_col].sum()
    )
    performance = {
        "Layer": key,
        "accuracy": accuracy_score(df_group[actual_col], df_group[predict_col]),
        "precision": precision_score(df_group[actual_col], df_group[predict_col], pos_label= 1),
        "recall": recall_score(df_group[actual_col], df_group[predict_col], pos_label= 1),
        "F1": f1_score(df_group[actual_col], df_group[predict_col]),
        "number_positive_uplift": len(df_group[actual_col][df_group[actual_col] == 1]),
        "number_negative_uplift": len(df_group[actual_col][df_group[actual_col] != 1]),
    }                                                                                                                                
    return performance

# COMMAND ----------

df_model = df_product_tpr.copy().drop(
    ["SEC_SALES", "SEC_BASELINE_ORG", "BANNER", "REGION"], axis=1
)

df_model = df_model.merge(
    df_ts_feature_total,
    on=["CATEGORY", "DPNAME", "DATE", "PRI_SALES_WITHOUT_BANDED"],
    how="left",
)

df_model = df_model.merge(
    df_ts_feature_baseline,
    on=["CATEGORY", "DPNAME", "DATE", "PRI_BASELINE"],
    how="left",
)

df_model = df_model.merge(
    df_trendline_feature[["CATEGORY", "DPNAME", "YEARWEEK", "TRENDLINE_DAILY"]],
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)
# df_model["TRENDLINE_DAILY"] = df_model["TRENDLINE_DAILY"] * df_model["DTWORKINGDAY"] # weekly

df_model = df_model.merge(
    df_phasing_features[
        [
            "CATEGORY",
            "DPNAME",
            "YEARWEEK",
            "RATIO_MEAN",
            "RATIO_MEDIAN",
            "RATIO_STD",
        ]
    ],
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)

df_model["ABS_UPLIFT"] = df_model["PRI_SALES_WITHOUT_BANDED"] - df_model["PRI_BASELINE"]
df_model["PCT_UPLIFT"] = np.where(
    df_model["PRI_BASELINE"] == 0, 0, df_model["ABS_UPLIFT"] / df_model["PRI_BASELINE"]
)

df_model["CLASSIFY_UPLIFT"] = np.where(
    df_model["ABS_UPLIFT"] >= 0, 1, -1
)  # positive = 1, negative = -1

print(df_model[df_model["PCT_UPLIFT"].between(-0.5, 0.5)].shape[0] / df_model.shape[0])
# df_model = df_model[
#     ((df_model["PCT_UPLIFT"].between(-1, 1)) & (df_model["YEARWEEK"] <= 202326))
#     | (df_model["YEARWEEK"] > 202326)
# ]
# df_model["ABS_UPLIFT"] = np.where(
#     df_model["PCT_UPLIFT"].between(-0.5, 0.5),
#     df_model["ABS_UPLIFT"],
#     df_model["PRI_BASELINE"] * 0.5,
# )

df_model = df_model.sort_values(["CATEGORY", "DPNAME", "YEARWEEK"]).reset_index(
    drop=True
)

# COMMAND ----------

# df_check = df_model.groupby(["YEARWEEK"])["CLASSIFY_UPLIFT"].value_counts().unstack().reset_index().fillna(0)

# df_check["total"] = df_check.iloc[:,-1] + df_check.iloc[:,-2]
# df_check["label_rate"] = df_check[1] / df_check["total"]
# df_check["distribution"] = df_check["total"] / df_check["total"].sum()
# display(df_check)
# df_model[df_model["YEARWEEK"].between(202201, 202352)].to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/df_raw.csv", index = False)

# COMMAND ----------

# train from end 12/2021 to end 6/2023
X_train = df_model[df_model["YEARWEEK"].between(202152, 202326)].drop(
    [
        "DATE",
        "PRI_SALES",
        "PRI_SALES_WITHOUT_BANDED",
        "ABS_UPLIFT",
        "PCT_UPLIFT",
        "CLASSIFY_UPLIFT",
    ],
    axis=1,
)
Y_train = df_model[df_model["YEARWEEK"].between(202152, 202326)]["CLASSIFY_UPLIFT"]

X_test = df_model[df_model["YEARWEEK"] > 202326].drop(
    [
        "DATE",
        "PRI_SALES",
        "PRI_SALES_WITHOUT_BANDED",
        "ABS_UPLIFT",
        "PCT_UPLIFT",
        "CLASSIFY_UPLIFT",
    ],
    axis=1,
)
Y_test = df_model[df_model["YEARWEEK"] > 202326]["CLASSIFY_UPLIFT"]

X_train.shape, Y_train.shape, X_test.shape, Y_test.shape

# COMMAND ----------

categorical_cols = X_train.select_dtypes(include=[object]).columns.values
print(categorical_cols)

X_train[X_train.columns.difference(categorical_cols)] = X_train[X_train.columns.difference(categorical_cols)].fillna(0)
X_test[X_test.columns.difference(categorical_cols)] = X_test[X_test.columns.difference(categorical_cols)].fillna(0)

X_train[categorical_cols] =X_train[categorical_cols].astype("category")
X_test[categorical_cols] =X_test[categorical_cols].astype("category") 
# X_train = X_train.drop(categorical_cols, axis = 1)
# X_test = X_test.drop(categorical_cols, axis = 1)

# COMMAND ----------

model = LGBMClassifier()
model.fit(X_train, Y_train)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## overall

# COMMAND ----------

y_pred = model.predict(X_test)
df_evaluate = X_test.copy()
df_evaluate["ACTUAL_UPLIFT"] = Y_test
df_evaluate["PREDICTED_UPLIFT"] = y_pred

df_evaluate[
    [
        "PRI_SALES",
        "PRI_SALES_WITHOUT_BANDED",
        "ABS_UPLIFT",
        "PCT_UPLIFT",
    ]
] = df_model[df_model["YEARWEEK"] > 202326][
    [
        "PRI_SALES",
        "PRI_SALES_WITHOUT_BANDED",
        "ABS_UPLIFT",
        "PCT_UPLIFT",
    ]
]

total_pfm_classfication(df_evaluate["ACTUAL_UPLIFT"], df_evaluate["PREDICTED_UPLIFT"])

# COMMAND ----------

f_importance = pd.DataFrame({'Feature_Name': X_train.columns, 'Weight_Importance': model.feature_importances_})

f_importance_sorted = f_importance.sort_values(by='Weight_Importance', ascending=False).reset_index(drop=True)
display(f_importance_sorted)

# COMMAND ----------

pd.DataFrame.from_dict(
        [
            classify_check(key, df_group, "ACTUAL_UPLIFT", "PREDICTED_UPLIFT")
            for key, df_group in  df_evaluate.groupby("CATEGORY")
        ]
)

# COMMAND ----------

def shap_viz(model, X_val, target):
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_val) 

    plt.figure(figsize=(18, 6))
    plt.subplot(1,2,1)
    shap.summary_plot(shap_values, X_val.values, plot_type='bar', class_names=["1","0"], feature_names=X_val.columns, max_display=20, show=False, plot_size=None)
    plt.title(f'– Weight of the impact of each feature')

    plt.subplot(1,2,2)
    shap.summary_plot(shap_values[1], X_val.values, feature_names=X_val.columns, max_display=20, show=False,  plot_size=None)
    plt.title(f' – Directional impact of each feature')
    plt.tight_layout()
    # plt.savefig(f'{title}.png', bbox_inches='tight', dpi=100)
    plt.show()

# COMMAND ----------

shap_viz(model, X_test , Y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uplift TPR transformation

# COMMAND ----------

y_pred = model.predict(X_test)
df_evaluate = X_test.copy()
df_evaluate["ACTUAL_CLASSIFY"] = Y_test
df_evaluate["PREDICTED_CLASSIFY"] = y_pred

df_evaluate[["PRI_SALES", "PRI_SALES_WITHOUT_BANDED", "ABS_UPLIFT"]] = df_model[
    df_model["YEARWEEK"] > 202326
][["PRI_SALES", "PRI_SALES_WITHOUT_BANDED", "ABS_UPLIFT"]]

df_evaluate = df_evaluate.merge(
    df_uplift_phasing[
        [
            "CATEGORY",
            "DPNAME",
            "YEARWEEK",
            "RATIO_MEAN_UPLIFT",
            "RATIO_MEDIAN_UPLIFT",
            "RATIO_STD_UPLIFT",
        ]
    ],
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)

# COMMAND ----------

# MAGIC %md
# MAGIC experiment calculate uplift TPR after classify pos/neg uplift

# COMMAND ----------

# df_evaluate["RATIO"] = 0
# for cat in tqdm(df_evaluate["DPNAME"].unique()):
#     pos_ratio = df_model[
#         (df_model["DPNAME"] == cat)
#         & (df_model["YEARWEEK"] <= 202326)
#         & (df_model["CLASSIFY_UPLIFT"] == 1)
#         & (df_model["PRI_BASELINE"] != 0)
#         & (df_model["PCT_UPLIFT"].between(-0.5, 0.5))
#     ]["PCT_UPLIFT"].mean()
#     df_evaluate["RATIO"] = np.where(
#         (df_evaluate["DPNAME"] == cat) & (df_evaluate["PREDICTED_CLASSIFY"] == 1),
#         pos_ratio,
#         df_evaluate["RATIO"],
#     )

#     neg_ratio = df_model[
#         (df_model["DPNAME"] == cat)
#         & (df_model["YEARWEEK"] <= 202326)
#         & (df_model["CLASSIFY_UPLIFT"] == -1)
#         & (df_model["PRI_BASELINE"] != 0)
#         & (df_model["PCT_UPLIFT"].between(-0.5, 0.5))
#     ]["PCT_UPLIFT"].mean()
#     df_evaluate["RATIO"] = np.where(
#         (df_evaluate["DPNAME"] == cat) & (df_evaluate["PREDICTED_CLASSIFY"] == -1),
#         neg_ratio,
#         df_evaluate["RATIO"],
#     )

# df_evaluate["PREDICTED_UPLIFT"] = df_evaluate["PRI_BASELINE"] * df_evaluate["RATIO"]

# df_evaluate["PREDICTED_UPLIFT"] = np.where(
#     df_evaluate["PREDICTED_CLASSIFY"] == 1,
#     df_evaluate["PRI_BASELINE"] * 0.73,
#     df_evaluate["PRI_BASELINE"] * -0.51,
# )
df_evaluate["PREDICTED_UPLIFT"] = (
    df_evaluate["PREDICTED_CLASSIFY"]
    * df_evaluate["RATIO_MEDIAN_UPLIFT"]
    * df_evaluate["PRI_BASELINE"]
)
df_evaluate["ACTUAL_TOTAL"] = df_evaluate["ABS_UPLIFT"] + df_evaluate["PRI_BASELINE"]
df_evaluate["PREDICTED_TOTAL"] = (
    df_evaluate["PREDICTED_UPLIFT"] + df_evaluate["PRI_BASELINE"]
)

# COMMAND ----------

# df_dp_zero_sales = df_evaluate.groupby("DPNAME")["ACTUAL_TOTAL"].sum().reset_index()
# df_dp_zero_sales = df_dp_zero_sales[df_dp_zero_sales["ACTUAL_TOTAL"] == 0]

print(performance_check("total", df_evaluate, "ABS_UPLIFT", "PREDICTED_UPLIFT"))
print(performance_check("total", df_evaluate, "ACTUAL_TOTAL", "PREDICTED_TOTAL"))
print(performance_check("total", df_evaluate, "ACTUAL_TOTAL", "PRI_BASELINE"))

# COMMAND ----------

accuracy_check("total", df_evaluate, "ACTUAL_TOTAL", ["PREDICTED_TOTAL","PRI_BASELINE"])

# COMMAND ----------

df_agg_total = df_evaluate.groupby("YEARWEEK")[["ACTUAL_TOTAL", "PREDICTED_TOTAL","PRI_BASELINE","ABS_UPLIFT","PREDICTED_UPLIFT"]].sum().reset_index()
df_agg_total["YEARWEEK"] = df_agg_total["YEARWEEK"].astype(str)
print(performance_check("total", df_agg_total, "ACTUAL_TOTAL", "PREDICTED_TOTAL"))
px.line(df_agg_total, x = "YEARWEEK", y = ["ACTUAL_TOTAL", "PREDICTED_TOTAL","PRI_BASELINE"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cate, bin

# COMMAND ----------

pd.DataFrame.from_dict(
        [
            performance_check(key, df_group, "ABS_UPLIFT", "PREDICTED_UPLIFT")
            for key, df_group in  df_evaluate.groupby("CATEGORY")
        ]
)

# COMMAND ----------

display(pd.DataFrame.from_dict(
        [
            performance_check(key, df_group, "ACTUAL_TOTAL", "PREDICTED_TOTAL")
            for key, df_group in  df_evaluate.groupby("CATEGORY")
        ]
))
display(pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, "ACTUAL_TOTAL", ["PREDICTED_TOTAL","PRI_BASELINE"])
            for key, df_group in  df_evaluate.groupby("CATEGORY")
        ]
))

# COMMAND ----------

df_evaluate["BIN_UPLIFT_qcut"] = pd.qcut(
    df_evaluate["ABS_UPLIFT"], q=10, duplicates = "drop"
)
print(df_evaluate["BIN_UPLIFT_qcut"].value_counts(normalize = True).sort_index())

pd.DataFrame.from_dict(
        [
            performance_check(key, df_group, "ABS_UPLIFT", "PREDICTED_UPLIFT")
            for key, df_group in  df_evaluate.groupby("BIN_UPLIFT_qcut")
        ]
).sort_values("Layer")

# COMMAND ----------

