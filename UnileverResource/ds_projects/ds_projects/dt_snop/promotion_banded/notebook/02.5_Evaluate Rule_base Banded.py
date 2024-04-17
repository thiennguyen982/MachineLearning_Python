# Databricks notebook source
# MAGIC %run "/Users/ng-minh-hoang.dat@unilever.com/Rule-base Banded Promotion contribute in Baseline/02_Phasing_Banded"

# COMMAND ----------

df_final.describe()

# COMMAND ----------

# df_final["TOTAL_SALES_MEAN"][
#     (df_final["VOLUME_BANDED_MEAN"] != 0)
#     & (df_final["TOTAL_SALES_MEAN"] > df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["PRI_SALES"])
# ] = df_final["FC_PRI_BASELINE_WEEKLY"]

# df_final["TOTAL_SALES_MEAN"][
#     (df_final["VOLUME_BANDED_MEAN"] != 0)
#     & (df_final["TOTAL_SALES_MEAN"] > df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] > df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["TOTAL_SALES_MEAN"])
# ] = df_final["FC_PRI_BASELINE_WEEKLY"]

# df_final["TOTAL_SALES_MEAN"][
#     (df_final["VOLUME_BANDED_MEAN"] != 0)
#     & (df_final["TOTAL_SALES_MEAN"] < df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] > df_final["TOTAL_SALES_MEAN"])
# ] = df_final["FC_PRI_BASELINE_WEEKLY"]

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

actual_col = "PRI_SALES"
predict_col_arr = ["TOTAL_SALES_MEAN", "TOTAL_SALES_MEDIAN"]

df_accuracy_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 9):
    df_pattern = df_final[
        (df_final["YEAR"] == 2023)
        & (df_final["MONTH"] == month_idx)
    ]
    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in df_pattern.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_TOTAL_SALES_MEAN": "sum_predictsale_mean_month_"
            + str(month_idx),
            "Accuracy_TOTAL_SALES_MEAN": "accuracy_mean_month_" + str(month_idx),
            "Error_TOTAL_SALES_MEAN": "error_mean_month_" + str(month_idx),
            "Sum_predictsale_TOTAL_SALES_MEDIAN": "sum_predictsale_median_month_"
            + str(month_idx),
            "Accuracy_TOTAL_SALES_MEDIAN": "accuracy_median_month_" + str(month_idx),
            "Error_TOTAL_SALES_MEDIAN": "error_median_month_" + str(month_idx),
        }
    )

    df_accuracy_monthly2023 = df_accuracy_monthly2023.merge(
        df_accuracy_phase_2Y, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_accuracy_monthly2023.replace([-np.inf, np.inf], 0, inplace=True)
df_accuracy_monthly2023.fillna(0, inplace=True)

df_accuracy_monthly2023[
    [
        "CATEGORY",
        "accuracy_mean_month_1",
        "accuracy_mean_month_2",
        "accuracy_mean_month_3",
        "accuracy_mean_month_4",
        "accuracy_mean_month_5",
        "accuracy_mean_month_6",
        "accuracy_mean_month_7",
        "accuracy_mean_month_8",
        # "accuracy_mean_month_9",
        # "accuracy_mean_month_10",
        "accuracy_median_month_1",
        "accuracy_median_month_2",
        "accuracy_median_month_3",
        "accuracy_median_month_4",
        "accuracy_median_month_5",
        "accuracy_median_month_6",
        "accuracy_median_month_7",
        "accuracy_median_month_8",
        # "accuracy_median_month_9",
        # "accuracy_median_month_10",
    ]
]

# COMMAND ----------

sum_acc = []
for i in range(1, 9):
    sum_acc.append(
        (
            df_accuracy_monthly2023[f"sum_actualsale_month_{i}"]
            * df_accuracy_monthly2023[f"accuracy_mean_month_{i}"]
        ).sum() / df_accuracy_monthly2023[f"sum_actualsale_month_{i}"].sum()
    )
print(sum_acc)
print("Result =", np.mean(sum_acc))

# COMMAND ----------

# df_accuracy_monthly2023.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/accuracy_baseline&banded_monthly2023.csv", index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Uplift phasing vs assump uplift

# COMMAND ----------

print(df_final["DPNAME"].nunique(), df_final["DPNAME"][df_final["DPNAME"].isin(df_temp["DPNAME"].unique())].nunique())

list_dp_promo = df_temp["DPNAME"].unique()

df_final[df_final["DPNAME"].isin(df_temp["DPNAME"].unique())].shape[0] / df_final.shape[0], df_final[df_final["DPNAME"].isin(df_temp["DPNAME"].unique())]["PRI_SALES"].sum() / df_final["PRI_SALES"].sum()

# COMMAND ----------

def accuracy_check_by_dp(key, df_group, actual_col, predict_col_arr, Ratio=1):
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

df_eval_uplift = df_final[["CATEGORY","DPNAME","YEARWEEK","PRI_SALES","FC_PRI_BASELINE_WEEKLY","TOTAL_SALES_MEAN","TIMERUN_PROMOTION","TYPE"]]

df_eval_uplift["UPLIFT"] = df_eval_uplift["PRI_SALES"] - df_eval_uplift["FC_PRI_BASELINE_WEEKLY"]
df_eval_uplift["P_UPLIFT"] = df_eval_uplift["PRI_SALES"] - df_eval_uplift["TOTAL_SALES_MEAN"]

# COMMAND ----------

df_eval_uplift.isnull().sum()

# COMMAND ----------

def describe_percent_uplift(df_eval_uplift, list_dp_promo, mode="DPNAME"):
    percent_desc_uplift = []

    for key, df_group in df_eval_uplift[
        df_eval_uplift["DPNAME"].isin(list_dp_promo)
    ].groupby(mode):
        df_group["UPLIFT"] = df_group["UPLIFT"].fillna(0)
        df_group["P_UPLIFT"] = df_group["P_UPLIFT"].fillna(0)

        neg_p_uplift = df_group[(df_group["P_UPLIFT"] < 0) & (df_group["TIMERUN_PROMOTION"].notnull())].shape[0] / df_group.shape[0]
        neg_p_uplift_neg_act_larger = (
            df_group[(df_group["P_UPLIFT"] < 0) & (df_group["UPLIFT"] < 0) & (df_group["P_UPLIFT"] > df_group["UPLIFT"])& (df_group["TIMERUN_PROMOTION"].notnull())].shape[0]
            / df_group.shape[0]
        )
        neg_p_uplift_neg_act_smaller = (
            df_group[(df_group["P_UPLIFT"] < 0) & (df_group["UPLIFT"] < 0) & (df_group["P_UPLIFT"] < df_group["UPLIFT"])& (df_group["TIMERUN_PROMOTION"].notnull())].shape[0]
            / df_group.shape[0]
        )
        neg_p_uplift_equal_act = (
            df_group[(df_group["P_UPLIFT"] < 0) & (df_group["UPLIFT"] == df_group["P_UPLIFT"])& (df_group["TIMERUN_PROMOTION"].notnull())].shape[0]
            / df_group.shape[0]
        )
        neg_p_uplift_pos_act = (
            df_group[(df_group["P_UPLIFT"] < 0) & (df_group["UPLIFT"] > 0) & (df_group["TIMERUN_PROMOTION"].notnull())].shape[0]
            / df_group.shape[0]
        )

        pos_p_uplift = df_group[(df_group["P_UPLIFT"] > 0)& (df_group["TIMERUN_PROMOTION"].notnull())].shape[0] / df_group.shape[0]
        pos_p_uplift_neg_act = (
            df_group[(df_group["P_UPLIFT"] > 0) & (df_group["UPLIFT"] < 0) & (df_group["P_UPLIFT"] > df_group["UPLIFT"])& (df_group["TIMERUN_PROMOTION"].notnull())].shape[0]
            / df_group.shape[0]
        )
        pos_p_uplift_equal_act = (
            df_group[(df_group["P_UPLIFT"] > 0) & (df_group["UPLIFT"] == df_group["P_UPLIFT"])& (df_group["TIMERUN_PROMOTION"].notnull())].shape[0]
            / df_group.shape[0]
        )
        pos_p_uplift_pos_act_larger = (
            df_group[(df_group["P_UPLIFT"] > 0) & (df_group["UPLIFT"] > 0) & (df_group["P_UPLIFT"] > df_group["UPLIFT"])& (df_group["TIMERUN_PROMOTION"].notnull())].shape[0]
            / df_group.shape[0]
        )
        pos_p_uplift_pos_act_smaller = (
            df_group[(df_group["P_UPLIFT"] > 0) & (df_group["UPLIFT"] > 0) & (df_group["P_UPLIFT"] < df_group["UPLIFT"])& (df_group["TIMERUN_PROMOTION"].notnull())].shape[0]
            / df_group.shape[0]
        )

        zero_p_uplift = (
            df_group[
                (df_group["P_UPLIFT"] == 0) & (df_group["TIMERUN_PROMOTION"].notnull())
            ].shape[0]
            / df_group.shape[0]
        )
        zero_p_uplift_neg_act = (
            df_group[
                (df_group["P_UPLIFT"] == 0)
                & (df_group["UPLIFT"] < 0)
                & (df_group["TIMERUN_PROMOTION"].notnull())
            ].shape[0]
            / df_group.shape[0]
        )
        zero_p_uplift_zero_act = (
            df_group[
                (df_group["P_UPLIFT"] == 0)
                & (df_group["UPLIFT"] == 0)
                & (df_group["TIMERUN_PROMOTION"].notnull())
            ].shape[0]
            / df_group.shape[0]
        )
        zero_p_uplift_pos_act = (
            df_group[
                (df_group["P_UPLIFT"] == 0)
                & (df_group["UPLIFT"] > 0)
                & (df_group["TIMERUN_PROMOTION"].notnull())
            ].shape[0]
            / df_group.shape[0]
        )

        not_promotion = (
            df_group[df_group["TIMERUN_PROMOTION"].isnull()].shape[0]
            / df_group.shape[0]
        )

        percent_desc_uplift.append(
            pd.DataFrame(
                data ={
                    "DPNAME": key,
                    "NEG_P_UPLIFT": [neg_p_uplift],
                    "NEG_P_UPLIFT_LARGER_NEG_UPLIFT": [neg_p_uplift_neg_act_larger],
                    "NEG_P_UPLIFT_SMALLER_NEG_UPLIFT": [neg_p_uplift_neg_act_smaller],
                    "NEG_P_UPLIFT_EQUAL_UPLIFT": [neg_p_uplift_equal_act],
                    "NEG_P_UPLIFT_HAVE_POS_UPLIFT": [neg_p_uplift_pos_act],
                    "POS_P_UPLIFT": [pos_p_uplift],
                    "POS_P_UPLIFT_HAVE_NEG_UPLIFT": [pos_p_uplift_neg_act],
                    "POS_P_UPLIFT_EQUAL_UPLIFT": [pos_p_uplift_equal_act],
                    "POS_P_UPLIFT_LARGER_POS_UPLIFT": [pos_p_uplift_pos_act_larger],
                    "POS_P_UPLIFT_SMALLER_POS_UPLIFT": [pos_p_uplift_pos_act_smaller],
                    "ZERO_P_UPLIFT": [zero_p_uplift],
                    "ZERO_P_UPLIFT_HAVE_NEG_UPLIFT": [zero_p_uplift_neg_act],
                    "ZERO_P_UPLIFT_HAVE_ZERO_UPLIFT": [zero_p_uplift_zero_act],
                    "ZERO_P_UPLIFT_HAVE_POS_UPLIFT": [zero_p_uplift_pos_act],
                    "NOT_PROMO": [not_promotion],
                }
            )
        )
    df_desc_uplift = pd.concat(percent_desc_uplift)
    return df_desc_uplift

# COMMAND ----------

df_desc_uplift_dp = describe_percent_uplift(df_eval_uplift, list_dp_promo = df_temp["DPNAME"].unique(), mode = "DPNAME")
display(df_desc_uplift_dp)

# COMMAND ----------

df_eval_downacc = df_eval_uplift[
    ((df_eval_uplift["P_UPLIFT"] < 0)
    & (df_eval_uplift["UPLIFT"] > 0)
    & (df_eval_uplift["TIMERUN_PROMOTION"].notnull())) |
    ((df_eval_uplift["P_UPLIFT"] < 0)
    & (df_eval_uplift["UPLIFT"] < 0)
    & (df_eval_uplift["P_UPLIFT"] < df_eval_uplift["UPLIFT"])
    & (df_eval_uplift["TIMERUN_PROMOTION"].notnull())) |
    ((df_eval_uplift["P_UPLIFT"] > 0)
    & (df_eval_uplift["UPLIFT"] > 0)
    & (df_eval_uplift["P_UPLIFT"] > df_eval_uplift["UPLIFT"])
    & (df_eval_uplift["TIMERUN_PROMOTION"].notnull()))
]
display(df_eval_downacc)

# COMMAND ----------

df_eval_EDA = (
    df_eval_uplift.groupby(["CATEGORY", "DPNAME"])[["PRI_SALES", "TOTAL_SALES_MEAN"]]
    .sum()
    .sort_values("PRI_SALES", ascending=False)
    .reset_index()
    .merge(
        df_eval_downacc.groupby(["CATEGORY", "DPNAME"])["TOTAL_SALES_MEAN"]
        .sum()
        .reset_index(),
        on=["CATEGORY", "DPNAME"],
    )
)

df_eval_EDA["PERCENT"] = (
    df_eval_EDA["TOTAL_SALES_MEAN_y"] / df_eval_EDA["TOTAL_SALES_MEAN_x"]
)
print(
    (df_eval_EDA["PRI_SALES"] * df_eval_EDA["PERCENT"]).sum()
    / df_eval_EDA["PRI_SALES"].sum()
)

df_eval_EDA.describe()

# COMMAND ----------

df_desc_uplift_dp.iloc[:, 1:].mean(), df_desc_uplift_dp.iloc[:, 1:].median()

# COMMAND ----------

display(
    pd.concat(
        [
            pd.melt(
                df_desc_uplift_dp,
                id_vars=["DPNAME", "NEG_P_UPLIFT"],
                value_vars=[
                    "NEG_P_UPLIFT_LARGER_NEG_UPLIFT",
                    "NEG_P_UPLIFT_SMALLER_NEG_UPLIFT",
                    "NEG_P_UPLIFT_EQUAL_UPLIFT",
                    "NEG_P_UPLIFT_HAVE_POS_UPLIFT",
                ],
                var_name="CONTRIBUTE_ACTUAL_UPLIFT_N",
                value_name="PERCENT_N",
            ).sort_values("DPNAME"),
            pd.melt(
                df_desc_uplift_dp,
                id_vars=["DPNAME", "POS_P_UPLIFT"],
                value_vars=[
                    "POS_P_UPLIFT_HAVE_NEG_UPLIFT",
                    "POS_P_UPLIFT_EQUAL_UPLIFT",
                    "POS_P_UPLIFT_LARGER_POS_UPLIFT",
                    "POS_P_UPLIFT_SMALLER_POS_UPLIFT",
                ],
                var_name="CONTRIBUTE_ACTUAL_UPLIFT_P",
                value_name="PERCENT_P",
            ).sort_values("DPNAME").drop("DPNAME", axis = 1),
            pd.melt(
                df_desc_uplift_dp,
                id_vars=["DPNAME", "ZERO_P_UPLIFT"],
                value_vars=[
                    "ZERO_P_UPLIFT_HAVE_NEG_UPLIFT",
                    "ZERO_P_UPLIFT_HAVE_ZERO_UPLIFT",
                    "ZERO_P_UPLIFT_HAVE_POS_UPLIFT",
                ],
                var_name="CONTRIBUTE_ACTUAL_UPLIFT_Z",
                value_name="PERCENT_Z",
            ).sort_values("DPNAME").drop("DPNAME", axis = 1),
        ],
        axis=1,
    )
    .merge(
        df_desc_uplift_dp[
            [
                "DPNAME",
                "NOT_PROMO",
            ]
        ],
        on=["DPNAME"],
    )
    .sort_values("DPNAME")
    .reset_index(drop=True)

)

# COMMAND ----------

df_desc_uplift_cate = describe_percent_uplift(
    df_eval_uplift, list_dp_promo=df_temp["DPNAME"].unique(), mode="CATEGORY"
)
display(df_desc_uplift_cate)

# COMMAND ----------

display(df_final[df_final["DPNAME"] == "OMO LIQUID MATIC CFT SS (POU) 3.7KG"][
    ["KEY","YEARWEEK","SEC_SALES","PRI_SALES","FC_PRI_BASELINE_WEEKLY","TYPE","TIMERUN_PROMOTION","DATE_PROMOTION","PROMOTION_PHASING_MEAN","VOLUME_BANDED_MEAN","SUM_BASELINE_IN_PROMO","TOTAL_SALES_MEAN","TOTAL_SALES_MEDIAN"]
])

# COMMAND ----------

for dpname in [
    "OMO LIQUID MATIC CFT SS (POU) 3.7KG",
    "OMO PINK 5500 GR",
    "OMO RED 6000 GR",
    "CLEAR SHAMPOO MEN COOL SPORT 650G",
    "DOVE CONDITIONER INTENSIVE REPAIR 620G",
    "SUNSILK SHAMPOO SOFT & SMOOTH 650G",
    "SUNLIGHT LEMON 750G",
    "SURF DW LEMON GRASS 3600G",
    "CLOSE-UP PASTE GREEN 230 GR",
    "KNORR FISH SAUCE MAINSTREAM 750ML",
    "DOVEGEL SAKURA500G",
    "HAZ SG ROYAL JELLY LILY 1.1KG",
    "LIFEBUOYHAND WASH TOTAL500G",
]:
    temp_plot = df_final[(df_final["DPNAME"] == dpname)]

    temp_temp_plot = temp_plot
    temp_temp_plot["TOTAL_SALES_MEAN"][temp_plot["TIMERUN_PROMOTION"].isnull()] = np.nan
    fig = go.Figure(
        data=[
            go.Scatter(x=temp_plot["DATE"], y=temp_plot["PRI_SALES"], name="PRI_SALES", marker = {'color' : 'blue'}),
            go.Scatter(x=temp_plot["DATE"], y=temp_plot["SEC_SALES"], name="SEC_SALES", marker = {'color' : 'green'}),
            go.Scatter(
                x=temp_temp_plot["DATE"],
                y=temp_temp_plot["TOTAL_SALES_MEAN"],
                name="BASELINE + UPLIFT",
                connectgaps=False,
                marker = {'color' : 'red'},
                mode = "lines+markers",
            ),
            go.Scatter(
                x=temp_plot["DATE"],
                y=temp_plot["FC_PRI_BASELINE_WEEKLY"],
                name="BASELINE",
                marker = {'color' : 'orange'},
                mode = "lines+markers",
            ),
        ]
    )

    mask = temp_plot["TIMERUN_PROMOTION"].isnull()
    temp_plot["mask"] = mask.cumsum()
    for _, df_group in temp_plot[(temp_plot["TIMERUN_PROMOTION"].notnull()) & (temp_plot["TYPE"] == "PRELOAD")].groupby(
        "mask"
    ):
        fig.add_vrect(
            x0=df_group["DATE"].min(),
            x1=df_group["DATE"].max(),
            annotation_text="Preload",
            annotation_position="top left",
            fillcolor="#EEE8AA",
            opacity=0.25,
            line_width=0.2,
        )

    for _, df_group in temp_plot[(temp_plot["TIMERUN_PROMOTION"].notnull()) & (temp_plot["TYPE"] == "POSTLOAD")].groupby(
        "mask"
    ):
        fig.add_vrect(
            x0=df_group["DATE"].min(),
            x1=df_group["DATE"].max(),
            annotation_text="Postload",
            annotation_position="top left",
            fillcolor="PaleGreen",
            opacity=0.25,
            line_width=0.2,
        )

    fig.update_layout(title=dpname, hovermode= "x unified")

    fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Baseline vs Act and (Base + uplift) vs Act

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### By Category

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Keep original phasing

# COMMAND ----------

actual_col = "PRI_SALES"
predict_col_arr = ["FC_PRI_BASELINE_WEEKLY"]

df_accuracy_baseline_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 11):
    df_pattern = df_final[
        (df_final["YEAR"] == 2023)
        & (df_final["MONTH"] == month_idx)
        & (df_final["DPNAME"].isin(list_dp_promo))
    ]
    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in df_pattern.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_FC_PRI_BASELINE_WEEKLY": "sum_predictsale_month_"
            + str(month_idx),
            "Accuracy_FC_PRI_BASELINE_WEEKLY": "accuracy_month_" + str(month_idx),
            "Error_FC_PRI_BASELINE_WEEKLY": "error_month_" + str(month_idx),
        }
    )

    df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023.merge(
        df_accuracy_phase_2Y, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_accuracy_baseline_monthly2023.replace([-np.inf, np.inf], 0, inplace=True)
df_accuracy_baseline_monthly2023.fillna(0, inplace=True)

df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023[
    [
        "CATEGORY",
        "accuracy_month_1",
        "accuracy_month_2",
        "accuracy_month_3",
        "accuracy_month_4",
        "accuracy_month_5",
        "accuracy_month_6",
        "accuracy_month_7",
        "accuracy_month_8",
        # "accuracy_month_9",
        # "accuracy_month_10",
    ]
]
display(df_accuracy_baseline_monthly2023)

# COMMAND ----------

actual_col = "PRI_SALES"
predict_col_arr = ["TOTAL_SALES_MEDIAN"]

df_accuracy_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 11):
    df_pattern = df_final[
        (df_final["YEAR"] == 2023)
        & (df_final["MONTH"] == month_idx)
        & (df_final["DPNAME"].isin(list_dp_promo))
    ]
    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in df_pattern.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_TOTAL_SALES_MEDIAN": "sum_predictsale_month_"
            + str(month_idx),
            "Accuracy_TOTAL_SALES_MEDIAN": "accuracy_month_" + str(month_idx),
            "Error_TOTAL_SALES_MEDIAN": "error_month_" + str(month_idx),
        }
    )

    df_accuracy_monthly2023 = df_accuracy_monthly2023.merge(
        df_accuracy_phase_2Y, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_accuracy_monthly2023.replace([-np.inf, np.inf], 0, inplace=True)
df_accuracy_monthly2023.fillna(0, inplace=True)

df_accuracy_monthly2023 = df_accuracy_monthly2023[
    [
        "CATEGORY",
        "accuracy_month_1",
        "accuracy_month_2",
        "accuracy_month_3",
        "accuracy_month_4",
        "accuracy_month_5",
        "accuracy_month_6",
        "accuracy_month_7",
        "accuracy_month_8",
        # "accuracy_month_9",
        # "accuracy_month_10",
    ]
]
display(df_accuracy_monthly2023)

# COMMAND ----------

display(pd.concat([df_accuracy_baseline_monthly2023, df_accuracy_monthly2023]).sort_values("CATEGORY"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Fix original phasing

# COMMAND ----------

df_final["TOTAL_SALES_MEAN"][
    (df_final["VOLUME_BANDED_MEAN"] != 0)
    & (df_final["TOTAL_SALES_MEAN"] > df_final["PRI_SALES"])
    & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["PRI_SALES"])
] = df_final["FC_PRI_BASELINE_WEEKLY"]

df_final["TOTAL_SALES_MEAN"][
    (df_final["VOLUME_BANDED_MEAN"] != 0)
    & (df_final["TOTAL_SALES_MEAN"] > df_final["PRI_SALES"])
    & (df_final["FC_PRI_BASELINE_WEEKLY"] > df_final["PRI_SALES"])
    & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["TOTAL_SALES_MEAN"])
] = df_final["FC_PRI_BASELINE_WEEKLY"]

df_final["TOTAL_SALES_MEAN"][
    (df_final["VOLUME_BANDED_MEAN"] != 0)
    & (df_final["TOTAL_SALES_MEAN"] < df_final["PRI_SALES"])
    & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["PRI_SALES"])
    & (df_final["FC_PRI_BASELINE_WEEKLY"] > df_final["TOTAL_SALES_MEAN"])
] = df_final["FC_PRI_BASELINE_WEEKLY"]

# COMMAND ----------

actual_col = "PRI_SALES"
predict_col_arr = ["TOTAL_SALES_MEAN"]

df_accuracy_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 11):
    df_pattern = df_final[
        (df_final["YEAR"] == 2023)
        & (df_final["MONTH"] == month_idx)
        & (df_final["DPNAME"].isin(list_dp_promo))
    ]
    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in df_pattern.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_TOTAL_SALES_MEAN": "sum_predictsale_month_"
            + str(month_idx),
            "Accuracy_TOTAL_SALES_MEAN": "accuracy_month_" + str(month_idx),
            "Error_TOTAL_SALES_MEAN": "error_month_" + str(month_idx),
        }
    )

    df_accuracy_monthly2023 = df_accuracy_monthly2023.merge(
        df_accuracy_phase_2Y, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_accuracy_monthly2023.replace([-np.inf, np.inf], 0, inplace=True)
df_accuracy_monthly2023.fillna(0, inplace=True)

df_accuracy_monthly2023 = df_accuracy_monthly2023[
    [
        "CATEGORY",
        "accuracy_month_1",
        "accuracy_month_2",
        "accuracy_month_3",
        "accuracy_month_4",
        "accuracy_month_5",
        "accuracy_month_6",
        "accuracy_month_7",
        "accuracy_month_8",
        "accuracy_month_9",
        "accuracy_month_10",
    ]
]
display(df_accuracy_monthly2023)

# COMMAND ----------

display(pd.concat([df_accuracy_baseline_monthly2023, df_accuracy_monthly2023]).sort_values("CATEGORY"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### By DP

# COMMAND ----------

actual_col = "PRI_SALES"
predict_col_arr = ["FC_PRI_BASELINE_WEEKLY"]

df_accuracy_baseline_monthly2023 = pd.DataFrame(columns=["DPNAME"])

for month_idx in range(1, 11):
    df_pattern = df_final[
        (df_final["YEAR"] == 2023)
        & (df_final["MONTH"] == month_idx)
        & (df_final["DPNAME"].isin(list_dp_promo))
    ]
    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check_by_dp(key, df_group, actual_col, predict_col_arr)
            for key, df_group in df_pattern.groupby("DPNAME")
        ]
    )

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_FC_PRI_BASELINE_WEEKLY": "sum_predictsale_month_"
            + str(month_idx),
            "Accuracy_FC_PRI_BASELINE_WEEKLY": "accuracy_month_" + str(month_idx),
            "Error_FC_PRI_BASELINE_WEEKLY": "error_month_" + str(month_idx),
        }
    )

    df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023.merge(
        df_accuracy_phase_2Y, on=["DPNAME"], how="outer"
    )

# COMMAND ----------

df_accuracy_baseline_monthly2023.replace([-np.inf, np.inf], 0, inplace=True)
df_accuracy_baseline_monthly2023.fillna(0, inplace=True)

df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023[
    [
        "DPNAME",
        "accuracy_month_1",
        "accuracy_month_2",
        "accuracy_month_3",
        "accuracy_month_4",
        "accuracy_month_5",
        "accuracy_month_6",
        "accuracy_month_7",
        "accuracy_month_8",
        # "accuracy_month_9",
        # "accuracy_month_10",
    ]
]

display(df_accuracy_baseline_monthly2023)

# COMMAND ----------

actual_col = "PRI_SALES"
predict_col_arr = ["TOTAL_SALES_MEDIAN"]

df_accuracy_monthly2023 = pd.DataFrame(columns=["DPNAME"])

for month_idx in range(1, 11):
    df_pattern = df_final[
        (df_final["YEAR"] == 2023)
        & (df_final["MONTH"] == month_idx)
        & (df_final["DPNAME"].isin(list_dp_promo))
    ]
    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check_by_dp(key, df_group, actual_col, predict_col_arr)
            for key, df_group in df_pattern.groupby("DPNAME")
        ]
    )

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_TOTAL_SALES_MEDIAN": "sum_predictsale_month_"
            + str(month_idx),
            "Accuracy_TOTAL_SALES_MEDIAN": "accuracy_month_" + str(month_idx),
            "Error_TOTAL_SALES_MEDIAN": "error_month_" + str(month_idx),
        }
    )

    df_accuracy_monthly2023 = df_accuracy_monthly2023.merge(
        df_accuracy_phase_2Y, on=["DPNAME"], how="outer"
    )

# COMMAND ----------

df_accuracy_monthly2023.replace([-np.inf, np.inf], 0, inplace=True)
df_accuracy_monthly2023.fillna(0, inplace=True)

df_accuracy_monthly2023 = df_accuracy_monthly2023[
    [
        "DPNAME",
        "accuracy_month_1",
        "accuracy_month_2",
        "accuracy_month_3",
        "accuracy_month_4",
        "accuracy_month_5",
        "accuracy_month_6",
        "accuracy_month_7",
        "accuracy_month_8",
        # "accuracy_month_9",
        # "accuracy_month_10",
    ]
]
display(df_accuracy_monthly2023)

# COMMAND ----------

display(pd.concat([df_accuracy_baseline_monthly2023, df_accuracy_monthly2023]).sort_values("DPNAME"))

# COMMAND ----------

