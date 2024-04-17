# Databricks notebook source
# MAGIC %run "/Users/ng-minh-hoang.dat@unilever.com/Rule-base Banded Promotion contribute in Baseline/02_Phasing_Banded"

# COMMAND ----------

display(df_uplift[df_uplift["DPNAME"] == "OMO LIQUID MATIC CFT SS (POU) 3.7KG"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # FC BL lag4

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr):
    max_accuracy = 0
    max_accuracy_col = "BLANK"
    for col in predict_col_arr:
        error = sum((df_group[actual_col] - df_group[col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        if accuracy > max_accuracy:
            max_accuracy = accuracy
            max_accuracy_col = col
    return {"KEY": key, "MAX_ACCURACY": max_accuracy, "COL": max_accuracy_col}

# COMMAND ----------

df_evaluate = pd.DataFrame()

for yearweek_cutoff in tqdm([*range(202301, 202348)]):
    if (yearweek_cutoff - 4 < 202301):
        time_training = yearweek_cutoff - 52
    else:
        time_training = yearweek_cutoff - 4

    df_cutoff = pd.read_csv(f"/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/TRAINING_TO_{time_training}.csv")
    #lag4
    df_cutoff = df_cutoff[df_cutoff["YEARWEEK"] == yearweek_cutoff]

    df_evaluate = pd.concat([df_evaluate, df_cutoff])

df_evaluate = df_evaluate.sort_values(["KEY","YEARWEEK"])
df_evaluate.head(2)

# COMMAND ----------

df_accuracy_method = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/ACC_BEST_2022.csv")

DF_ERROR = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/ACC_ERROR_2022.csv")
DF_ERROR.shape, df_accuracy_method.shape

# COMMAND ----------

display(df_accuracy_method)

# COMMAND ----------

df_evaluate = df_evaluate.merge(df_accuracy_method, on = "KEY")
df_evaluate["FC_lag4_BL_Sec_daily"] = 0
for col in df_accuracy_method["COL"].unique():
    if col in df_evaluate.columns:
        df_evaluate["FC_lag4_BL_Sec_daily"].loc[(df_evaluate["COL"] == col)] = df_evaluate[col]

df_evaluate["FC_lag4_BL_Sec_weekly"] = df_evaluate["FC_lag4_BL_Sec_daily"] * df_evaluate["WORKINGDAY"]

# COMMAND ----------

df_check_missing = pd.pivot_table(df_evaluate, columns = "YEARWEEK", index = "KEY", values = "FC_lag4_BL_Sec_weekly")
df_check_missing.shape, df_check_missing.isnull().sum().sum(), df_check_missing.isnull().sum().sum() / df_check_missing.size

# COMMAND ----------

display(df_check_missing.reset_index())

# COMMAND ----------

df_evaluate[["BANNER","CATEGORY","DPNAME"]] = df_evaluate["KEY"].str.split("|", expand = True)
df_evaluate.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # adhoc

# COMMAND ----------

# df_temp = pd.read_parquet(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE.parquet"
# )

# DF_NATIONWIDE = (
#     df_temp.groupby(["CATEGORY", "DPNAME", "YEARWEEK"])[["ACTUALSALE", "BASELINE"]]
#     .sum()
#     .reset_index()
# )
# DF_NATIONWIDE["KEY"] = (
#     "NATIONWIDE|" + DF_NATIONWIDE["CATEGORY"] + "|" + DF_NATIONWIDE["DPNAME"]
# )
# # DF = DF.append(DF_NATIONWIDE)
# df_temp = DF_NATIONWIDE  # Only run for NATIONWIDE
# df_temp = df_temp[["CATEGORY","DPNAME", "YEARWEEK", "ACTUALSALE", "BASELINE"]]

# #######################################

# df_calendar = pd.read_excel(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx",
#     sheet_name="BASEWEEK-CALENDAR",
#     engine="openpyxl",
# )
# df_calendar = df_calendar[["YEARWEEK", "DTWORKINGDAY"]]

# df_temp["FUTURE"] = "N"
# df_temp = df_temp.merge(df_calendar, on="YEARWEEK")
# df_temp["WORKINGDAY"] = df_temp["DTWORKINGDAY"]
# df_temp.describe()

# COMMAND ----------

df_temp = df_sec_sales.copy()
df_temp[df_temp["YEARWEEK"] <= 202349].describe()

# COMMAND ----------

df_temp[df_temp["YEARWEEK"].between(202301, 202347)].shape, df_temp[df_temp["YEARWEEK"].between(202301, 202347)]["DPNAME"].nunique()

# COMMAND ----------

df_missing_dp = pd.read_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Rule-base Banded Promotion contribute in Baseline/Missing DP Name in ML BL.xlsx", skiprows = 0)

df_missing_dp.columns = ["CATEGORY","DPNAME"]
df_missing_dp["DPNAME"] = df_missing_dp["DPNAME"].str.upper()
df_missing_dp.dropna(how = "any", inplace = True)
df_missing_dp

# COMMAND ----------

t_dp = df_missing_dp[df_missing_dp["DPNAME"].isin(df_temp[df_temp["YEARWEEK"].between(202301, 202347)]["DPNAME"].unique())]
print(t_dp.shape[0])
display(df_missing_dp[~df_missing_dp["DPNAME"].isin(t_dp["DPNAME"].unique())])

# COMMAND ----------

df_missing_dp = t_dp.copy()
print(df_missing_dp["DPNAME"].nunique())

print(
    "Missing have raised",
    df_missing_dp[df_missing_dp["DPNAME"].isin(df_evaluate["DPNAME"].unique())][
        "DPNAME"
    ].nunique(),
)
display(df_missing_dp[df_missing_dp["DPNAME"].isin(df_evaluate["DPNAME"].unique())])

df_missing_dp = df_missing_dp[
    ~df_missing_dp["DPNAME"].isin(df_evaluate["DPNAME"].unique())
]
df_missing_dp["DPNAME"].nunique()

# COMMAND ----------

df_check = df_temp[
    (df_temp["DPNAME"].isin(df_missing_dp["DPNAME"].unique()))
    & (df_temp["YEARWEEK"] <= 202347)
]
df_check["YEAR"] = df_check["YEARWEEK"] // 100
df_check["DPNAME"].nunique(), df_check.shape

# COMMAND ----------

df_group_check = df_check.groupby(["CATEGORY","DPNAME","YEAR"]).agg({"YEAR":["count"], "YEARWEEK" : ["unique"]}).reset_index()
df_group_check.columns = ["CATEGORY","DPNAME","YEAR","YEAR_COUNT", "YEAR_VALUES"]
display(df_group_check)

# COMMAND ----------

df_group_check["ERROR"] = "BLANK"
threshold = 5
df_error_raise = pd.DataFrame(columns = df_group_check.columns)

for key, df_group in df_group_check.groupby(["CATEGORY","DPNAME"]):
    if sum(df_group[df_group["YEAR"] < 2022]["YEAR_COUNT"]) <= threshold:
        df_group["ERROR"] = f"Data have less than {threshold} non-NaN rows before 2022 for training"

    if sum(df_group[df_group["YEAR"] == 2022]["YEAR_COUNT"]) <= threshold:
        df_group["ERROR"] = f"Data have less than {threshold} non-NaN rows in 2022 for Evaluate FC method"
    
    if sum(df_group[df_group["YEAR"] < 2023]["YEAR_COUNT"]) <= threshold:
        df_group["ERROR"] = f"Data have less than {threshold} non-NaN rows before 2023 for Forecasting"
    
    df_error_raise = pd.concat([df_error_raise, df_group])

# COMMAND ----------

display(df_error_raise)

# COMMAND ----------

for error_reason in df_error_raise["ERROR"].unique():
    print(df_error_raise[df_error_raise["ERROR"] == error_reason]["DPNAME"].nunique(), error_reason)

# COMMAND ----------

display(df_error_raise[["CATEGORY","DPNAME","ERROR"]].drop_duplicates())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Convert Sec2Pri BL

# COMMAND ----------

df_convert_lag4 = df_sec_sales[["YEARWEEK","ACTUALSALE","BASELINE","CATEGORY","DPNAME"]].copy()
df_convert_lag4 = df_convert_lag4.rename(
    columns={"ACTUALSALE": "SEC_SALES", "BASELINE": "SEC_BASELINE"}
)

df_convert_lag4 = df_convert_lag4.merge(
    df_pri_sales[["CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]],
    how="outer",
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
)
df_convert_lag4 = df_convert_lag4.rename(columns={"ACTUALSALE": "PRI_SALES"})

df_convert_lag4 = df_convert_lag4[
    (df_convert_lag4["YEARWEEK"] <= df_evaluate["YEARWEEK"].max())
    & (df_convert_lag4["YEARWEEK"] >= 202101)
]

# COMMAND ----------

df_convert_lag4.describe(include = "all")

# COMMAND ----------

df_convert_lag4 = df_convert_lag4.merge(
    df_evaluate[["CATEGORY","DPNAME","YEARWEEK","FC_lag4_BL_Sec_weekly"]], on=["CATEGORY", "DPNAME", "YEARWEEK"], 
    how="outer",
)
# print(df_missing_dp[df_missing_dp["DPNAME"].isin(df_convert_lag4[df_convert_lag4["YEARWEEK"].between(202301, 202347)]["DPNAME"].unique())]["DPNAME"].nunique())

df_convert_lag4 = df_convert_lag4[(df_convert_lag4["YEARWEEK"] < 202301) | (df_convert_lag4["FC_lag4_BL_Sec_weekly"].notnull())]

# print(df_missing_dp[df_missing_dp["DPNAME"].isin(df_convert_lag4[df_convert_lag4["YEARWEEK"].between(202301, 202347)]["DPNAME"].unique())]["DPNAME"].nunique())

df_convert_lag4.head(2)

# COMMAND ----------

df_convert_lag4["BANNER"] = "NATIONWIDE"
df_convert_lag4["KEY"] = "NATIONWIDE|" + df_convert_lag4["CATEGORY"] + "|" + df_convert_lag4["DPNAME"]

df_convert_lag4 = df_convert_lag4.merge(df_calendar_workingday, on="YEARWEEK")

df_convert_lag4["DATE"] = pd.to_datetime(
    (df_convert_lag4["YEARWEEK"]).astype(str) + "-1", format="%G%V-%w"
)

df_convert_lag4 = df_convert_lag4.merge(df_week_master, on="YEARWEEK")

df_convert_lag4["QUARTER"] = ((df_convert_lag4["MONTH"] - 1) / 3).astype(int) + 1

df_convert_lag4 = df_convert_lag4.drop_duplicates(
    subset=["KEY", "YEARWEEK"], keep="first"
)

df_convert_lag4["WEEK/MONTH COUNT"] = (
    df_convert_lag4.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("count")
    .astype(int)
)
df_convert_lag4["WEEK/MONTH ORDER"] = (
    df_convert_lag4.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("rank")
    .astype(int)
)
df_convert_lag4 = df_convert_lag4.sort_values(["KEY", "YEARWEEK"]).reset_index(
    drop=True
)

df_convert_lag4["FC_PRI_BASELINE"] = 0
df_convert_lag4["FC_PRI_BASELINE_WEEKLY"] = 0
df_convert_lag4["SELLINGDAY_WEEK/MONTH RATIO_median"] = 0
df_convert_lag4.head(2)

# COMMAND ----------

df_convert_lag4[df_convert_lag4["YEARWEEK"] >= 202301].describe(include = "all")

# COMMAND ----------

for month_idx in range(1,12):
    year_idx = 2023
    
    phasing_lower = df_convert_lag4["YEARWEEK"][
        (df_convert_lag4["YEAR"] >= (year_idx - 2)) & (df_convert_lag4["MONTH"] == month_idx)
    ].min()
    # phasing_lower = 202101

    if month_idx == 1:
        phasing_upper = df_convert_lag4["YEARWEEK"][
            df_convert_lag4["YEAR"] <= (year_idx - 1)
        ].max()
    else:
        phasing_upper = df_convert_lag4["YEARWEEK"][
            (df_convert_lag4["YEAR"] <= year_idx) & (df_convert_lag4["MONTH"] == (month_idx - 1))
        ].max()
        
    df_ratio_phasing = df_convert_lag4[
        (df_convert_lag4["YEARWEEK"] >= phasing_lower) & (df_convert_lag4["YEARWEEK"] <= phasing_upper)
    ]

    df_ratio_WEEK_MONTH = create_ratio_phasing(df_ratio_phasing, "PRI_SALES")

    df_convert_pattern = convert_data(
        df_convert_lag4[
            (df_convert_lag4["YEAR"] == year_idx) & (df_convert_lag4["MONTH"] == month_idx)
        ],
        df_ratio_WEEK_MONTH,
        input_var = "FC_lag4_BL_Sec_weekly",
        output_var = "FC_PRI_BASELINE_WEEKLY"
    )
    df_convert_pattern = df_convert_pattern.reset_index(drop=True)  
    indices = df_convert_lag4[(df_convert_lag4["YEAR"] == year_idx) & (df_convert_lag4["MONTH"] == month_idx)].index 
    positions = df_convert_lag4.index.get_indexer(indices) 
    df_convert_lag4.iloc[positions] = df_convert_pattern

# COMMAND ----------

df_convert_lag4.tail()

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio=1):
    df_group[actual_col] = df_group[actual_col].fillna(0)

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"CATEGORY": key, "Sum_actualsale": sum_actualsale}

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col].fillna(0)
        df_group[predict_col] = df_group[predict_col].replace([-np.inf, np.inf], 0)
        df_group[predict_col] = df_group[predict_col] * Ratio

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        sum_predictsale = df_group[predict_col].sum()

        performance["Sum_predictsale_" + predict_col] = sum_predictsale
        performance["Accuracy_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error

    return performance

# COMMAND ----------

accuracy_check("Baseline", df_convert_lag4[df_convert_lag4["YEARWEEK"].between(202301, 202347)], "PRI_SALES", ["FC_PRI_BASELINE_WEEKLY"])

# COMMAND ----------

df_export = df_convert_lag4[df_convert_lag4["YEARWEEK"] >= 202301]
df_export["BANNER"] == "NATIONWIDE"
df_export = df_export[["YEARWEEK","BANNER","CATEGORY","DPNAME","DATE","YEAR","MONTH","WEEK/MONTH COUNT","WEEK/MONTH ORDER","SEC_SALES","PRI_SALES","FC_lag4_BL_Sec_weekly","FC_PRI_BASELINE_WEEKLY","SELLINGDAY_WEEK/MONTH RATIO_median"]]

df_export.columns = ["YEARWEEK","BANNER","CATEGORY","DPNAME","DATE","YEAR","MONTH","WEEK_COUNT","WEEK_ORDER","ACTUAL_SEC_SALES","ACTUAL_PRI_SALES","FC_Lag4_BL_SEC","FC_Lag4_BL_PRIM","RATIO_median"]
df_export.shape

# COMMAND ----------

display(df_export)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Merge BL lag4 with Banded

# COMMAND ----------

df_uplift = df_uplift[df_uplift["DPNAME"] != "SURF LIQUID FLORAL POUCH 2.9KG"]
promo_code_2023 = promo_code_2023[promo_code_2023["DPNAME"].isin(df_uplift["DPNAME"].unique())]

df_temp = df_uplift.merge(
    promo_code_2023.drop("MATERIAL", axis=1), on=["CATEGORY", "DPNAME"], how="inner"
)
df_temp = df_temp.sort_values(
    ["CATEGORY", "DPNAME", "START_DATE_PROMOTION", "TIMERUN_PROMOTION"]
).reset_index(drop=True)

df_temp["WEEKDAY_OVER_6"] = (7 - df_temp["START_DATE_PROMOTION"].dt.isocalendar()["day"]).astype(int)
df_temp["START_DATE_PROMOTION"][df_temp["WEEKDAY_OVER_6"].astype(int) < 1] = df_temp["START_DATE_PROMOTION"] + pd.to_timedelta(df_temp["WEEKDAY_OVER_6"] + 1, unit= "D")

df_temp["DATE_PROMOTION"] = df_temp["START_DATE_PROMOTION"] + pd.to_timedelta(
    df_temp["TIMERUN_PROMOTION"] - 1, unit="W"
)
df_temp["YEARWEEK"] = df_temp["DATE_PROMOTION"].dt.isocalendar()["year"] * 100 + df_temp["DATE_PROMOTION"].dt.isocalendar()["week"]

# COMMAND ----------

df_temp.head(2)

# COMMAND ----------

df_export = df_export.merge(df_temp, on=["CATEGORY", "DPNAME", "YEARWEEK"], how="left")

df_export["VOLUME_BANDED_MEDIAN"] = df_export["PROMOTION_PHASING_MEDIAN"] * np.where(
    df_export["TYPE"] == "PRELOAD",
    df_export["VOLUME_PRELOAD_MEDIAN"],
    df_export["VOLUME_POSTLOAD_MEDIAN"],
)
df_export["VOLUME_BANDED_MEDIAN"] = df_export["VOLUME_BANDED_MEDIAN"].fillna(0)

df_export["SUM_BASELINE_PRI_IN_PROMO"] = 0
df_export["SUM_BASELINE_PRI_IN_PROMO"][df_export["TYPE"] == "PRELOAD"] = (
    df_export[(df_export["TIMERUN_PROMOTION"] > 0) & (df_export["TYPE"] == "PRELOAD")]
    .groupby(["CATEGORY", "DPNAME", "START_DATE_PROMOTION"])["FC_Lag4_BL_PRIM"]
    .transform("sum")
    .astype(float)
)

df_export["SUM_BASELINE_PRI_IN_PROMO"][df_export["TYPE"] == "POSTLOAD"] = (
    df_export[(df_export["TIMERUN_PROMOTION"] > 0) & (df_export["TYPE"] == "POSTLOAD")]
    .groupby(["CATEGORY", "DPNAME", "START_DATE_PROMOTION"])["FC_Lag4_BL_PRIM"]
    .transform("sum")
    .astype(float)
)

df_export["FC_Lag4_Banded_PRIM"] = df_export["SUM_BASELINE_PRI_IN_PROMO"] * df_export["VOLUME_BANDED_MEDIAN"]

df_export["TOTAL_PRIM_SALES"] = (
    df_export["FC_Lag4_BL_PRIM"] + df_export["FC_Lag4_Banded_PRIM"]
)

df_export["TOTAL_PRIM_SALES"][df_export["TOTAL_PRIM_SALES"].isnull()] = df_export["FC_Lag4_BL_PRIM"]

# df_export["SUM_BASELINE_SEC_IN_PROMO"] = 0
# df_export["SUM_BASELINE_SEC_IN_PROMO"] = (
#     df_export[(df_export["TIMERUN_PROMOTION"] > 0)]
#     .groupby(["CATEGORY", "DPNAME", "START_DATE_PROMOTION"])["FC_Lag4_BL_SEC"]
#     .transform("sum")
#     .astype(float)
# )
df_export["FC_Lag4_Banded_SEC"] = df_export["FC_Lag4_BL_SEC"] * df_export["VOLUME_UPLIFT_MEDIAN"]

df_export["TOTAL_SEC_SALES"] = (
    df_export["FC_Lag4_BL_SEC"] + df_export["FC_Lag4_Banded_SEC"]
)

df_export["TOTAL_SEC_SALES"][df_export["TOTAL_SEC_SALES"].isnull()] = df_export["FC_Lag4_BL_SEC"]

df_export = df_export.drop_duplicates(subset=["CATEGORY","DPNAME", "YEARWEEK"])
df_export = df_export.fillna(0)

# COMMAND ----------

df_export.head(2)

# COMMAND ----------

# df_export.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DF_SETUP_BL&BANDED.csv", index = False)

# COMMAND ----------

df_export = df_export[
    [
        "YEARWEEK",
        "BANNER",
        "CATEGORY",
        "DPNAME",
        "DATE",
        "YEAR",
        "MONTH",
        "WEEK_COUNT",
        "WEEK_ORDER",
        "ACTUAL_SEC_SALES",
        "ACTUAL_PRI_SALES",
        "FC_Lag4_BL_SEC",
        "FC_Lag4_BL_PRIM",
        "FC_Lag4_Banded_PRIM",
        "FC_Lag4_Banded_SEC",
        "TOTAL_PRIM_SALES",
        "TOTAL_SEC_SALES",
    ]
]

# COMMAND ----------

df_export.describe()

# COMMAND ----------

# df_export.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DF_EVALUATE_BL&BANDED.csv", index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Evaluation

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Category

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio=1):
    df_group[actual_col] = df_group[actual_col].fillna(0)

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"CATEGORY": key, "Sum_actualsale": sum_actualsale}

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col].fillna(0)
        df_group[predict_col] = df_group[predict_col].replace([-np.inf, np.inf], 0)
        df_group[predict_col] = df_group[predict_col] * Ratio

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        sum_predictsale = df_group[predict_col].sum()

        performance["Sum_predictsale_" + predict_col] = sum_predictsale
        performance["Accuracy_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error

    return performance

# COMMAND ----------

actual_col = "ACTUAL_PRI_SALES"
predict_col_arr = [
    "FC_Lag4_BL_PRIM","TOTAL_PRIM_SALES"
]
df_acc_cate_pri = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 12):
    df_convert_pattern = df_export[
        (df_export["YEAR"] == 2023)
        & (df_export["MONTH"] == month_idx)
    ]
    df_accuracy = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in  df_convert_pattern.groupby("CATEGORY")
        ]
    )

    for col in df_accuracy.columns:
        if col != "CATEGORY":
            df_accuracy = df_accuracy.rename(columns = {
                col: col + "_month_" + str(month_idx)
            })

    df_acc_cate_pri = df_acc_cate_pri.merge(
        df_accuracy, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_acc_cate_pri

# COMMAND ----------

accuracy_check("total", df_export, "ACTUAL_PRI_SALES", [
    "FC_Lag4_BL_PRIM","TOTAL_PRIM_SALES"
])

# COMMAND ----------

actual_col = "ACTUAL_PRI_SALES"
predict_col_arr = [
    "FC_Lag4_BL_SEC","TOTAL_SEC_SALES"
]
df_acc_cate_sec = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 12):
    df_convert_pattern = df_export[
        (df_export["YEAR"] == 2023)
        & (df_export["MONTH"] == month_idx)
    ]
    df_accuracy = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in  df_convert_pattern.groupby("CATEGORY")
        ]
    )

    for col in df_accuracy.columns:
        if col != "CATEGORY":
            df_accuracy = df_accuracy.rename(columns = {
                col: col + "_month_" + str(month_idx)
            })

    df_acc_cate_sec = df_acc_cate_sec.merge(
        df_accuracy, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_acc_cate_sec

# COMMAND ----------

accuracy_check("total", df_export, "ACTUAL_SEC_SALES", [
    "FC_Lag4_BL_SEC","TOTAL_SEC_SALES"
])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## DPNAME

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio=1):
    df_group[actual_col] = df_group[actual_col].fillna(0)

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"DPNAME": key, "Sum_actualsale": sum_actualsale}

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col].fillna(0)
        df_group[predict_col] = df_group[predict_col].replace([-np.inf, np.inf], 0)
        df_group[predict_col] = df_group[predict_col] * Ratio

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        sum_predictsale = df_group[predict_col].sum()

        performance["Sum_predictsale_" + predict_col] = sum_predictsale
        performance["Accuracy_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error

    return performance

# COMMAND ----------

actual_col = "ACTUAL_PRI_SALES"
predict_col_arr = [
    "FC_Lag4_BL_PRIM","TOTAL_PRIM_SALES"
]
df_acc_dp_pri = pd.DataFrame(columns=["DPNAME"])

for month_idx in range(1, 12):
    df_convert_pattern = df_export[
        (df_export["YEAR"] == 2023)
        & (df_export["MONTH"] == month_idx)
    ]
    df_accuracy = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in  df_convert_pattern.groupby("DPNAME")
        ]
    )

    for col in df_accuracy.columns:
        if col != "DPNAME":
            df_accuracy = df_accuracy.rename(columns = {
                col: col + "_month_" + str(month_idx)
            })

    df_acc_dp_pri = df_acc_dp_pri.merge(
        df_accuracy, on=["DPNAME"], how="outer"
    )

# COMMAND ----------

df_acc_dp_pri

# COMMAND ----------

actual_col = "ACTUAL_PRI_SALES"
predict_col_arr = [
    "FC_Lag4_BL_SEC","TOTAL_SEC_SALES"
]
df_acc_dp_sec = pd.DataFrame(columns=["DPNAME"])

for month_idx in range(1, 12):
    df_convert_pattern = df_export[
        (df_export["YEAR"] == 2023)
        & (df_export["MONTH"] == month_idx)
    ]
    df_accuracy = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in  df_convert_pattern.groupby("DPNAME")
        ]
    )

    for col in df_accuracy.columns:
        if col != "DPNAME":
            df_accuracy = df_accuracy.rename(columns = {
                col: col + "_month_" + str(month_idx)
            })

    df_acc_dp_sec = df_acc_dp_sec.merge(
        df_accuracy, on=["DPNAME"], how="outer"
    )

# COMMAND ----------

df_acc_dp_sec

# COMMAND ----------

# with pd.ExcelWriter("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Rule-base Banded Promotion contribute in Baseline/DF_EVALUATE_BL&BANDED.xlsx") as writer:
   
#     df_export.to_excel(writer, sheet_name="Result", index = False)
#     df_acc_cate_pri.to_excel(writer, sheet_name="Accuracy_Prim_Cate", index = False)
#     df_acc_cate_sec.to_excel(writer, sheet_name="Accuracy_Sec_Cate", index = False)
#     df_acc_dp_pri.to_excel(writer, sheet_name="Accuracy_Prim_DP", index = False)
#     df_acc_dp_sec.to_excel(writer, sheet_name="Accuracy_Sec_DP", index = False)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

