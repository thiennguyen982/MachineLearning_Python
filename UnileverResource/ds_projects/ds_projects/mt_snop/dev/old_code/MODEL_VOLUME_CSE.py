# Databricks notebook source
# MAGIC %pip install category_encoders==2.6.0 pandas==1.5.0

# COMMAND ----------

dbutils.widgets.text("BANNER_REMOVE_REPEATED", "")
dbutils.widgets.text("BANNER_PROMOTION", "")
dbutils.widgets.text("BANNER_NAME", "")
dbutils.widgets.text("USER_TRIGGER", "")

BANNER_REMOVE_REPEATED = dbutils.widgets.get("BANNER_REMOVE_REPEATED")
BANNER_PROMOTION = dbutils.widgets.get("BANNER_PROMOTION")
BANNER_NAME = dbutils.widgets.get("BANNER_NAME")
USER_TRIGGER = dbutils.widgets.get("USER_TRIGGER")

# BANNER_REMOVE_REPEATED = "2022_AEON REMOVE REPEATED FILE.parquet.gzip_Lai-Trung-Minh Duc.PDF"
# BANNER_PROMOTION = "2022_PROMOTION TRANSFORMATION_UPDATEDAEON.par_Lai-Trung-Minh Duc.PDF"
# BANNER_NAME = "AEON"
# USER_TRIGGER = "Lai-Trung-Minh.Duc@unilever.com"

for check_item in [BANNER_REMOVE_REPEATED, BANNER_PROMOTION, BANNER_NAME, USER_TRIGGER]:
    if check_item == "" or check_item == None:
        raise Exception("STOP PROGRAM - NO DATA TO RUN")

print(f"BANNER_REMOVE_REPEATED: {BANNER_REMOVE_REPEATED}")
print(f"BANNER_PROMOTION: {BANNER_PROMOTION}")
print(f"BANNER_NAME: {BANNER_NAME}")
print(f"USER_TRIGGER: {USER_TRIGGER}")

# COMMAND ----------

# ENVIRONMENT VARIABLE FROM AZURE DATA FACTORY
from datetime import datetime
import time

import warnings

warnings.filterwarnings("ignore")

# COMMAND ----------

import pandas as pd
import numpy as np
import traceback
import re
import sys
import category_encoders as ce

import sys

sys.stdout.fileno = lambda: False
import ray

runtime_env = {
    "pip": [
        "lightgbm",
        "xgboost",
        "scipy",
        "sktime==0.7.0",
        "category_encoders==2.6.0",
        "pandas==1.5.0",
    ]
}
ray.init(include_dashboard=False, ignore_reinit_error=True, runtime_env=runtime_env)

# COMMAND ----------

# DBTITLE 1,CREATE TRAIN AND TEST, FC
def create_train_test(df):
    def split_data(df):
        df.loc[df["TYPE"] == "finished", "TYPE"] = "model"
        df.loc[df["TYPE"] == "ongoing", "TYPE"] = "fc"
        df.loc[df["TYPE"] == "incoming", "TYPE"] = "fc"
        return df

    def mapping_month(date):
        year = date.year
        month = date.month
        day = date.day
        if month >= 25:
            new = month + 1
            if new > 12:
                new = 1
        else:
            new = month
        return str(month)

    def mapping_year(date):
        year = date.year
        month = date.month
        day = date.day
        if month >= 25:
            new = month + 1
            if new > 12:
                new_year = year + 1
            else:
                new_year = year
        else:
            new_year = year
        return str(new_year)

    def split_model_fc(df):
        df["MONTHPOST"] = df["ORDER START DATE"].apply(lambda x: mapping_month(x))
        df["YEARPOST"] = df["ORDER START DATE"].apply(lambda x: mapping_year(x))
        df = df.sort_values(by=["YEARPOST", "MONTHPOST"]).reset_index(drop=True)
        df["YEARMONTHPOST"] = (
            df["MONTHPOST"].astype(str) + "/" + df["YEARPOST"].astype(str)
        )
        df["MONTHPOST"] = df["MONTHPOST"].astype(int)
        df["YEARPOST"] = df["YEARPOST"].astype(int)
        df_model = df[df["TYPE"] == "model"].reset_index(drop=True)
        df_fc = df[df["TYPE"] == "fc"].reset_index(drop=True)
        return df_model, df_fc

    def main_run():
        df_new = split_data(df)
        df_model, df_fc = split_model_fc(df_new)
        max_lst = list(df_model["YEARMONTHPOST"].unique())[-3:]
        df_train = df_model[~df_model["YEARMONTHPOST"].isin(max_lst)].reset_index(
            drop=True
        )
        df_test = df_model[df_model["YEARMONTHPOST"].isin(max_lst)].reset_index(
            drop=True
        )
        return df_model, df_fc, df_train, df_test

    return main_run()

# COMMAND ----------

# DBTITLE 1,ENCODE FEATURE
def feature_engineer(df, ls_encode, ls_dum, target):
    from scipy.special import boxcox1p, boxcox, inv_boxcox
    import category_encoders as ce

    def replace_ls(ls_1, ls_2):
        return dict(zip(ls_1, ls_2))

    def dumy_code(df, col):  # create dummy code column for list of dummny
        df[col] = df[col].apply(lambda x: str(x).lower())
        if col == "ON/OFF POST":
            list_dum = ["ON", "OFF"]
        else:
            list_dum = df[col].unique()
        for promo in list_dum:
            df[promo] = np.where(df[col] == promo.lower(), 1, 0)
        return df

    def dummy_processing(df, col_dum):
        df = dumy_code(df, col_dum)
        df = df.drop(col_dum, axis=1)
        return df

    def target_encode(
        df_train, df_test, ls_encode, target
    ):  # Encode feature in ls encode
        X_train = df_train.drop(target, axis=1)
        X_test = df_test.drop(target, axis=1)
        y_train = df_train[[target]]
        y_train[target] = y_train[target].apply(lambda x: boxcox1p(x, 0.5))
        y_test = df_test[[target]]

        for col in ls_encode:
            cbe_encoder = ce.leave_one_out.LeaveOneOutEncoder()
            cbe_encoder.fit(X_train[col], y_train[target])
            ls_1 = X_train[col]
            X_train[col] = cbe_encoder.transform(X_train[col])
            ls_2 = X_train[col]
            replace_ = replace_ls(ls_1, ls_2)
            X_test[col] = X_test[col].map(replace_)
            X_test = X_test.fillna(0)
        return X_train, X_test, y_train, y_test

    def main_run():
        df_new = df.fillna(0)
        for col_dum in ls_dum:  # convert feature into dummy columns
            df_new = dumy_code(df_new, col_dum)
            df_new = df_new.drop(col_dum, axis=1)

        df_model, df_fc, df_train, df_test = create_train_test(df_new)
        X_train, X_test, y_train, y_test = target_encode(
            df_train, df_test, ls_encode, target
        )
        X_train_fc, X_fc, y_train_fc, y_fc = target_encode(
            df_model, df_fc, ls_encode, target
        )

        return (
            X_train,
            X_test,
            y_train,
            y_test,
            df_test,
            X_train_fc,
            X_fc,
            y_train_fc,
            df_train,
            df_fc,
        )

    return main_run()

# COMMAND ----------

# DBTITLE 1,MODEL RUN
def model_running(X_train, X_test, y_train):
    from sklearn.linear_model import Lasso
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.neighbors import KNeighborsRegressor
    import xgboost as xgb
    import lightgbm as lgb
    from sklearn import linear_model
    from sklearn.tree import DecisionTreeClassifier
    from xgboost import XGBClassifier
    from scipy.special import boxcox1p, boxcox, inv_boxcox
    import category_encoders as ce

    def ls_model_run():

        linear = linear_model.LassoLars(alpha=0.1, normalize=False)

        rf = RandomForestRegressor()
        gbr = GradientBoostingRegressor()

        xgb_model = xgb.XGBRegressor()

        lgb_model = lgb.LGBMRegressor(num_boost_round=2000)
        lasso = Lasso(alpha=0.05)
        knn = KNeighborsRegressor(n_neighbors=12)

        xgb_class = XGBClassifier()
        ls_model = [lasso, xgb_model, rf, gbr, lgb_model]
        return ls_model

    def model_dev(X_train, X_test, y_train, ls_model):

        print(f"X_train: {X_train.shape}")
        print(f"X_test: {X_test.shape}")
        print(f"y_train: {y_train.shape}")
        print(f"ls_model: {ls_model}")

        df_predicted = pd.DataFrame()

        if X_test.shape[0] > 0:  # If Data in Future predict available
            X_train_fit = X_train.drop(
                [
                    "POST NAME",
                    "CATEGORY",
                    "ORDER START DATE",
                    "YEARMONTHPOST",
                    "TYPE",
                    "YEAR",
                    "ORDER END DATE",
                ],
                axis=1,
            )
            X_test_fit = X_test.drop(
                [
                    "POST NAME",
                    "CATEGORY",
                    "ORDER START DATE",
                    "YEARMONTHPOST",
                    "TYPE",
                    "YEAR",
                    "ORDER END DATE",
                ],
                axis=1,
            )
            for model in ls_model:
                model_ = model
                model_.fit(X_train_fit, y_train)
                predict = model.predict(X_test_fit)
                name = str(model)[:10]
                df_predicted[name] = predict
            df_predicted["MEDIAN"] = df_predicted.median(axis=1)
            for col in list(df_predicted.columns):
                df_predicted[col] = inv_boxcox(df_predicted.loc[:, [col]], 0.5) - 1
            for col in list(df_predicted.columns)[
                :-1
            ]:  # Get median of each model predict with median of total predict
                df_predicted[col] = df_predicted.loc[:, [col, "MEDIAN"]].median(axis=1)

        return df_predicted

    def main_run():
        ls_model = ls_model_run()
        df_predicted = model_dev(X_train, X_test, y_train, ls_model)
        return df_predicted

    return main_run()

# COMMAND ----------

# DBTITLE 1,MODEL SELECTION
def model_setection(df_predicted, target):
    def fa_dp(df_dp, col):
        # df_predict['predict']=df_predict['predict'].apply(lambda x:change_negative(x))
        df_dp["ABS"] = df_dp[col] - df_dp["ACTUAL"]
        df_dp["ABS"] = df_dp["ABS"].apply(lambda x: abs(x))
        sum_abs = df_dp["ABS"].sum()
        sum_predict = df_dp[col].sum()
        sum_actual = df_dp["ACTUAL"].sum()
        fa = 1 - sum_abs / sum_actual
        fb = (sum_actual - sum_predict) / sum_predict
        return fa, fb

    def model_selection(
        df_predicted,
    ):  ## Select model for each DP Name, for each DP NAME select the best FA and FB,
        ## if DP NAME match the condition of FA , FB => PASS, else : FAIL => Just for reference
        dp_result = pd.DataFrame()
        for dp in df_predicted["DP NAME"].unique():
            df_dp = df_predicted[df_predicted["DP NAME"] == dp]
            select = pd.DataFrame(index=[0, 1, 2, 3, 4])
            select["MODEL"] = 0
            select["FA"] = 0
            select["FB"] = 0
            i = 0
            a = float("inf")
            b = float("-inf")
            for col in [
                "XGBRegress",
                "RandomFore",
                "GradientBo",
                "LGBMRegres",
                "Lasso(alph",
            ]:
                fa, fb = fa_dp(df_dp, col)
                if fb == b or fb == a:
                    fb = 100
                if fa == a or fa == b:
                    fa = 0
                select["MODEL"][i] = col
                select["FA"][i] = fa * 100
                select["FB"][i] = fb * 100
                i = i + 1
            select_1 = select[(select["FB"] < 20) & (select["FB"] > -20)]
            if len(select_1 != 0):
                result = select_1.loc[[select_1["FA"].idxmax()]]
                result["DP NAME"] = dp
                result["FB CHECK"] = "PASS"
            else:
                result = select.loc[[select["FA"].idxmax()]]
                result["DP NAME"] = dp
                result["FB CHECK"] = "FAIL"
            dp_result = pd.concat([dp_result, result]).reset_index(drop=True)
        return dp_result

    def final_selection(
        df_predicted, target
    ):  # final selection model base on the rule with good FA andFB for each DP Name
        dp_result = model_selection(df_predicted)
        df_final = pd.DataFrame()
        for i in dp_result.index:
            dp_final = pd.DataFrame()
            model = dp_result["MODEL"][i]
            dp = dp_result["DP NAME"][i]
            df_dp = df_predicted[df_predicted["DP NAME"] == dp].reset_index()
            dp_final = df_dp.loc[
                :,
                [
                    model,
                    "ACTUAL",
                    "DP NAME",
                    "POST NAME",
                    "CATEGORY",
                    "YEARMONTHPOST",
                    "YEAR",
                ],
            ]
            dp_final["PREDICT"] = dp_final[model]
            dp_final["FC"] = dp_final["PREDICT"]
            dp_final = dp_final.drop(model, axis=1)
            df_final = pd.concat([df_final, dp_final])
            df_final[target] = df_final["ACTUAL"]
        return df_final, dp_result

    def main_run():
        df_final, dp_result = final_selection(df_predicted, target)
        return df_final, dp_result

    return main_run()

# COMMAND ----------

# DBTITLE 1,METRIC TO EVALUATION
def fa_testing(df_predict):
    from statistics import median

    def fa_cate(df_predict, target):
        df_predict.loc[df_predict["PREDICT"] < 0, "PREDICT"] = 0
        df_predict["ABS"] = df_predict["PREDICT"] - df_predict["EST. DEMAND (CS)"]
        df_predict["ABS"] = df_predict["ABS"].apply(lambda x: abs(x))
        df_fa = pd.DataFrame()
        for cate in df_predict["CATEGORY"].unique():
            df_fa_cate = pd.DataFrame()
            print(cate)
            df_cate = df_predict[df_predict["CATEGORY"] == cate]
            fa_month = []
            fb_month = []
            df_cate_by_month = pd.DataFrame(
                columns=["CATEGORY", "MONTH_POST", "FA", "FB"], index=[0]
            )
            for month in df_cate["YEARMONTHPOST"].unique():
                df_month = df_cate[df_cate["YEARMONTHPOST"] == month]
                fa = 1 - df_month["ABS"].sum() / df_month[target].sum()
                fb = (df_month[target].sum() - df_month["PREDICT"].sum()) / df_month[
                    "PREDICT"
                ].sum()
                print({"FA": (month, fa)})
                print({"FB": (month, fb)})
                df_cate_by_month["CATEGORY"][0] = cate
                df_cate_by_month["MONTH_POST"][0] = month
                df_cate_by_month["FA"][0] = fa
                df_cate_by_month["FB"][0] = fb
                df_fa_cate = pd.concat([df_fa_cate, df_cate_by_month])
                fa_month.append(fa)
                fb_month.append(fb)
            print({"FA": (cate, median(fa_month))})
            print({"FB": (cate, median(fb_month))})
            df_fa = pd.concat([df_fa, df_fa_cate])
        return df_fa

    def main_run():
        df_fa = fa_cate(df_predict, target)
        return df_fa

    return main_run()

# COMMAND ----------

# DBTITLE 1,RUN MODEL
def run_total(df, ls_encode, ls_dum, target, cate):
    df = df[df["CATEGORY"].isin(cate)]
    if len(df) == 0:
        df_total = pd.DataFrame()
        return df_total
    else:
        (
            X_train,
            X_test,
            y_train,
            y_test,
            df_test,
            X_train_fc,
            X_fc,
            y_train_fc,
            df_train,
            df_fc,
        ) = feature_engineer(df, ls_encode, ls_dum, target)
        df_predicted = model_running(X_train, X_test, y_train)
        df_predicted["ACTUAL"] = df_test[target]
        df_predicted["DP NAME"] = df_test["DP NAME"]
        df_predicted["ACTUAL SALE (CS)"] = df_test["ACTUAL SALE (CS)"]
        df_predicted["POST NAME"] = df_test["POST NAME"]
        df_predicted["YEARPOST"] = df_test["YEARPOST"]
        df_predicted["CATEGORY"] = df_test["CATEGORY"]
        df_predicted["YEARMONTHPOST"] = df_test["YEARMONTHPOST"]
        df_predicted["YEAR"] = df_test["YEAR"]
        df_predicted["CODE TYPE"] = df_test["TYPE"]
        # df_predicted["ON/OFF POST"] = df_test["ON/OFF POST"]
        df_final, dp_result = model_setection(df_predicted, target)

        df_forecast = model_running(X_train_fc, X_fc, y_train_fc)
        df_forecast["DP NAME"] = df_fc["DP NAME"]
        df_forecast["POST NAME"] = df_fc["POST NAME"]
        df_forecast["ACTUAL SALE (CS)"] = df_fc["ACTUAL SALE (CS)"]
        df_forecast["CATEGORY"] = df_fc["CATEGORY"]
        df_forecast["YEARMONTHPOST"] = df_fc["YEARMONTHPOST"]
        df_forecast["YEAR"] = df_fc["YEAR"]
        df_forecast["PREDICT"] = 0
        df_forecast["ORDER END DATE"] = df_fc["ORDER END DATE"]
        df_forecast["CODE TYPE"] = df_fc["TYPE"]
        # df_forecast["ON/OFF POST"] = df_test["ON/OFF POST"]
        ls_result = dict(zip(list(dp_result["DP NAME"]), dp_result["MODEL"]))
        for key in df_forecast["DP NAME"].unique():
            if key not in list(ls_result.keys()):
                dp_name = key
                df_forecast.loc[
                    df_forecast["DP NAME"] == dp_name, "PREDICT"
                ] = df_forecast.loc[df_forecast["DP NAME"] == dp_name, "MEDIAN"]
            else:
                dp_name = key
                model = ls_result[key]
                df_forecast.loc[
                    df_forecast["DP NAME"] == dp_name, "PREDICT"
                ] = df_forecast.loc[df_forecast["DP NAME"] == dp_name, model]

        df_train["PREDICT"] = 0
        df_final["KEY"] = df_final[["DP NAME", "YEAR", "POST NAME"]].agg(
            "|".join, axis=1
        )
        df_final_1 = df_final[["KEY", "PREDICT"]]
        df_test["KEY"] = df_test[["DP NAME", "YEAR", "POST NAME"]].agg("|".join, axis=1)
        df_test = df_test.merge(df_final_1, on="KEY", how="left")
        df_fc["PREDICT"] = df_forecast["PREDICT"]
        df_total = pd.concat([df_train, df_test])
        df_total = pd.concat([df_total, df_fc])
        # df_total=df_total.reset_index(drop=True)
        df_fa_cate = fa_testing(df_final)
        print(df_fa_cate)
        return df_total


@ray.remote
def REMOTE_run_total(df, ls_encode, ls_dum, target, cate):
    import warnings

    warnings.filterwarnings("ignore")
    return run_total(df, ls_encode, ls_dum, target, cate)

# COMMAND ----------

# DBTITLE 1,UPDATE THE CHANGE OF CLASS 
import os


def update_classification_product(df, MASTER_CLASS, df_master_quater):
    def class_preprocessing(text):
        if "c" in text:
            text = "c"
        return text

    def convert_format(df):
        df.columns = map(lambda x: str(x).upper(), df.columns)
        df = df.apply(lambda x: x.astype(str).str.lower())
        return df

    def detect_file(MASTER_CLASS):
        ls = os.listdir(MASTER_CLASS)
        file_name = pd.DataFrame()
        for i in ls:
            if "SKU complexity working file" in i:
                quater = i[-11:-5]
                df_class = pd.read_excel(
                    MASTER_CLASS + i,
                    engine="openpyxl",
                    sheet_name="Classification",
                    header=1,
                )
                df_class = df_class.loc[:, ["DP Name", "Class MT"]]
                df_class = df_class.dropna()
                df_class["QUATER"] = quater
                df_class = convert_format(df_class)
                df_class["CLASS MT"] = df_class["CLASS MT"].apply(
                    lambda x: class_preprocessing(x)
                )
                file_name = pd.concat([file_name, df_class])
        return file_name

    def extract_quater(df):
        df["ORDER START DATE"] = pd.to_datetime(df["ORDER START DATE"], "%d%m%Y")
        df["ORDER END DATE"] = pd.to_datetime(df["ORDER END DATE"], format="%d%m%Y")
        df["QUATER_START"] = pd.PeriodIndex(df["ORDER START DATE"], freq="Q")
        df["QUATER_END"] = pd.PeriodIndex(df["ORDER END DATE"], freq="Q")
        df["QUATER"] = df["QUATER_END"].astype(str)
        df["DIFF"] = df["QUATER_START"] - df["QUATER_END"]
        df["DIFF"] = df["DIFF"].astype(str)
        df["DIFF"] = df["DIFF"].apply(lambda x: x[1:3])
        df["DIFF"] = df["DIFF"].astype(int)
        return df

    def update_class_quater(df_master_quater, df):
        df_master_quater["START DATE OF QUARTER"] = pd.to_datetime(
            df_master_quater["START DATE OF QUARTER"], format="%d%m%Y"
        )
        df = extract_quater(df)
        df = df.merge(df_master_quater, on="QUATER", how="inner")
        df_duplicated = df[df["DIFF"] == -1]
        df_no_duplicated = df[df["DIFF"] != -1]
        df_duplicated["DISTANCE_QUATER"] = (
            df_duplicated["ORDER END DATE"] - df_duplicated["START DATE OF QUARTER"]
        ).apply(lambda x: x.days)
        df_duplicated["DISTANCE_QUATER_START"] = (
            df_duplicated["ORDER START DATE"] - df_duplicated["START DATE OF QUARTER"]
        ).apply(lambda x: x.days)
        df_duplicated["DISTANCE_QUATER_START"] = df_duplicated[
            "DISTANCE_QUATER_START"
        ].apply(lambda x: abs(x))
        df_duplicated["SELECTED"] = df_duplicated["DISTANCE_QUATER"].combine(
            df_duplicated["DISTANCE_QUATER_START"], func=lambda x, y: x > y
        )
        df_duplicated["QUATER"] = np.where(
            df_duplicated["SELECTED"] == "TRUE",
            df_duplicated["QUATER_END"],
            df_duplicated["QUATER_START"],
        )
        df_final = pd.concat([df_duplicated, df_no_duplicated])
        df_final["QUATER"] = df_final["QUATER"].apply(lambda x: str(x).lower())
        df_final = df_final.sort_values(by="ORDER START DATE")
        return df_final

    def merge_class(df_update, df_class):
        df_update["KEY"] = df_update[["DP NAME", "QUATER"]].agg("-".join, axis=1)
        df_class["KEY"] = df_class[["DP NAME", "QUATER"]].agg("-".join, axis=1)
        df_class = df_class.loc[:, ["KEY", "CLASS MT"]]
        df_update = df_update.merge(df_class, on="KEY", how="left")
        df_update["CLASS MT"] = df_update["CLASS MT"].fillna(0)
        df_update["CLASS"] = df_update["CLASS"].apply(lambda x: class_preprocessing(x))
        df_update["CLASS"] = np.where(
            df_update["CLASS MT"] == 0, df_update["CLASS"], df_update["CLASS MT"]
        )
        return df_update

    def main_run():
        df_update = update_class_quater(df_master_quater, df)
        df_class = detect_file(MASTER_CLASS)
        df_update_class = merge_class(df_update, df_class)
        df_update_class = merge_class(df_update, df_class)
        return df_update_class

    return main_run()

# COMMAND ----------

# DBTITLE 1,READ FILE OF MASTER AND TOTAL BANNER
MASTER_QUATER = f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/QUATER_MASTER.xlsx"
MASTER_CLASS = f"/dbfs/mnt/adls/MT_POST_MODEL/DATA_PROMOTION/CLASS_UPDATE/"

df_master_quater = pd.read_excel(MASTER_QUATER, engine="openpyxl")  # MASTER

df = pd.read_parquet(
    f"/dbfs/mnt/adls/MT_POST_MODEL/DEMAND_POST_MODEL/{BANNER_NAME}.parquet"
)  # DEMAND

df["TYPE"] = df["STATUS"]
target = "EST. DEMAND (CS)"

df_update_class_new = update_classification_product(
    df, MASTER_CLASS, df_master_quater
)  # UPDATE CLASS FOR EACH BANNER
ls_banner = df["BANNER"].unique()

# COMMAND ----------

# DBTITLE 1,SELECT COLUMNS FOR MODLE
ls_col = [
    "EST. DEMAND (CS)",
    "PRICE CHANGE",
    "ACTUAL SALE (CS)",
    # "MNTH_SIN",
    "KEY_EVENT",
    "DAY",
    "NSO",
    "CLOSE",
    "WEEK",
    "MONTH",
    "DAY_BEFORE_HOLIDAY_CODE",
    "%TTS",
    "% BMI",
    "POST_LOST",
    # "% KA INVESTMENT (ON TOP)",
    "WEEKDAY",
    "DAY_AFTER_HOLIDAY_CODE",
    "MTWORKINGSUM",
    "COVID_IMPACT",
    # "DAY_SIN",
    # "DAY_COS",
    "CATEGORY",
    "POST_DISTANCE",
    "POST NAME",
    "MONTH_TOTAL",
    "WEEK_TOTAL",
    "DAY_TOTAL",
    "WEEKMONTH",
    "TYPE",
    "ORDER START DATE",
    "YEAR",
    "ORDER END DATE",
]  #
ls_encode = [
    "SPECIAL_EVENT",
    "GIFT TYPE",
    "PORTFOLIO (COTC)",
    "PROMOTION GROUPING",
    "PACKSIZE",
    "PACKGROUP",
    "BRAND VARIANT",
    "SUBBRAND",
    "BRAND",
    "SEGMENT",
    "DP NAME",
    "FORMAT",
    "PRODUCTION SEGMENT",
    "PROMOTION_IMPACT",
]
ls_dum = [
    "ON/OFF POST",
    "CLASS",
]


ls_select = ls_col + ls_encode + ls_dum
df_update_class_new = df_update_class_new.loc[:, ls_select]

# COMMAND ----------

df_update_class_new["TYPE"].unique()

# COMMAND ----------

# DBTITLE 1,RUN FOR  BANNER
df_final_result = pd.DataFrame()

banner = BANNER_NAME.lower()
"""
Train data by division - not category individually. 
Performance is better.
"""
if banner == "metro":
    ls_cate = [
        ["skincare"],
        ["hair"],
        ["skincleansing"],
        ["oral"],  # PC
        ["savoury"],  # FOOD
        ["hnh"],
        ["fabsen"],
        ["fabsol"],
    ]  # HC]
else:
    ls_cate = [
        ["skincare"],
        ["hair"],
        ["skincleansing"],
        ["oral"],
        ["deo"],
        ["savoury"],
        ["hnh"],
        ["fabsen"],
        ["fabsol"],
    ]

LS_TASK = [
    REMOTE_run_total.remote(df_update_class_new, ls_encode, ls_dum, target, cate)
    for cate in ls_cate
]
LS_TASK = ray.get(LS_TASK)
df_final_result = pd.concat(LS_TASK)

# COMMAND ----------

ray.shutdown()

# COMMAND ----------

df_merge = df_update_class_new[
    ["ON/OFF POST", "CLASS", "POST NAME", "DP NAME", "YEAR", "MONTH_TOTAL"]
]
# df_merge['MONTH'] = df_merge['MONTH_TOTAL']
df_merge["YEAR"] = df_merge["YEAR"].astype(str)
df_merge["KEY"] = (
    df_merge["DP NAME"] + "-" + df_merge["POST NAME"] + "-" + df_merge["YEAR"]
)
df_merge = df_merge[["ON/OFF POST", "CLASS", "KEY"]]

# COMMAND ----------

df_final_result["YEAR"] = df_final_result["YEAR"].astype(str)
df_final_result["KEY"] = (
    df_final_result["DP NAME"]
    + "-"
    + df_final_result["POST NAME"]
    + "-"
    + df_final_result["YEAR"]
)

# COMMAND ----------

df_final_result = df_final_result.merge(df_merge, on="KEY", how="left")


# COMMAND ----------

df_final_result["BANNER"] = banner

# COMMAND ----------

df_final_result = df_final_result.sort_values(by ='ORDER START DATE')
df_final_result

# COMMAND ----------

df_final_result.to_parquet(
    f"/dbfs/mnt/adls/MT_POST_MODEL/RESULT/RESULT_{BANNER_NAME}.parquet"
)

# COMMAND ----------

import requests
import base64
import time

url = "https://prod-215.westeurope.logic.azure.com:443/workflows/72d08b2ad319428f9927758f1d87f892/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=erridJccQ8dln3JknLJxgbgnR55TCPDrNwqdWrd10CU"

with open(
    f"/dbfs/mnt/adls/MT_POST_MODEL/RESULT/RESULT_{BANNER_NAME}.parquet",
    "rb",
) as my_file:
    encoded_string = base64.b64encode(my_file.read()).decode("utf-8")

my_obj = {
    "KEY": f"RESULT_{BANNER_NAME}_{int(time.time())}.parquet",
    "USER": USER_TRIGGER,
    "BANNER": BANNER_NAME,
    "FILEINDEX_CONTENT": encoded_string,
}

requests.post(url, json=my_obj)

# COMMAND ----------

