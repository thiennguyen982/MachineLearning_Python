# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data

# COMMAND ----------

# MAGIC %run "/Users/ng-minh-hoang.dat@unilever.com/Promotion_TPR_Primary_Sales/01_preprocessing_data"

# COMMAND ----------

df_backup = df_product_tpr.copy()

print(df_product_tpr.shape)
df_product_tpr.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Overview 

# COMMAND ----------

print(df_product_tpr[df_product_tpr["PRI_BASELINE_WEEKLY"] > df_product_tpr["PRI_SALES"]].shape[0] / df_product_tpr.shape[0])

print(df_product_tpr[df_product_tpr["PRI_BASELINE_WEEKLY"] > df_product_tpr["PRI_SALES_WITHOUT_BANDED"]].shape[0] / df_product_tpr.shape[0])

# COMMAND ----------

df_product_tpr.describe(include = "all", percentiles = [0.05, 0.1, 0.25, 0.75, 0.9, 0.95])

# COMMAND ----------

yw_max = df_product_tpr["YEARWEEK"].max()
# yw_min = df_product_tpr["YEARWEEK"].min()
yw_min = 202152
print(
    f"Coverage of Promotion TPR ({yw_min} - {yw_max})",
    df_product_tpr[df_product_tpr["PROMOTION_count"] > 0].shape[0]
    / df_product_tpr[df_product_tpr["YEARWEEK"].between(yw_min, yw_max)].shape[0],
)

print(
    df_product_tpr[df_product_tpr["PROMOTION_count"] > 0][
        ["PRI_SALES", "SEC_SALES", "PRI_BASELINE_WEEKLY"]
    ].sum()
    / df_product_tpr[df_product_tpr["YEARWEEK"].between(yw_min, yw_max)][
        ["PRI_SALES", "SEC_SALES", "PRI_BASELINE_WEEKLY"]
    ].sum()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Viz

# COMMAND ----------

df_groupby = df_product_tpr[(df_product_tpr["PROMOTION_count"] > 0)].groupby(["CATEGORY","DPNAME"])[["PRI_SALES","PRI_SALES_WITHOUT_BANDED"]].sum().reset_index().sort_values("PRI_SALES", ascending = False)
display(df_groupby)

# COMMAND ----------

for dpname in [
    #large sales
    "OMO LIQUID MATIC CFT SS (POU) 3.7KG",
    "OMO RED 6000 GR",
    "SUNLIGHT LEMON 3600G",
    # medium sales ~100 TON
    # "CLEAR SHAMPOO MEN COOL SPORT 370G",
    # "SURF PRO 9000 GR",
]:
    temp_plot = df_product_tpr[(df_product_tpr["DPNAME"] == dpname)]

    fig = go.Figure(
        data=[
            # go.Scatter(x=temp_plot["DATE"], y=temp_plot["PRI_SALES"], name="PRI_SALES", marker = {'color' : 'blue'}),
            go.Scatter(x=temp_plot["DATE"], y=temp_plot["PRI_SALES_WITHOUT_BANDED"], name="PRI_BASELINE", marker = {'color' : 'green'}),
            go.Bar(
                x=temp_plot["DATE"],
                y=temp_plot["PROMOTION_count"],
                name="number of promotion",
            ),
        ]
    )

    fig.update_layout(title=dpname, hovermode= "x unified")

    fig.show()

# COMMAND ----------

for i, cate in enumerate(df_product_tpr["CATEGORY"].unique()):
    plt.figure(i)
    fig, ax = plt.subplots(1, 3, figsize = (20, 6))
    temp_plot = df_product_tpr[(df_product_tpr["CATEGORY"] == cate)]
    temp_plot = temp_plot[temp_plot["YEARWEEK"].between(202152, 202352)]

    sns.barplot(x = temp_plot["WEEK_OF_QUARTER"], y = temp_plot["PRI_SALES_WITHOUT_BANDED"], ax=ax[0])
    sns.barplot(x = temp_plot["WEEK_OF_MONTH"], y = temp_plot["PRI_SALES_WITHOUT_BANDED"], ax=ax[1])
    sns.barplot(x = temp_plot["MONTH_OF_QUARTER"], y = temp_plot["PRI_SALES_WITHOUT_BANDED"], ax=ax[2])
    ax[1].set_title(cate)

# COMMAND ----------

for i, cate in enumerate(df_product_tpr["CATEGORY"].unique()):
    plt.figure(i)
    fig, ax = plt.subplots(1, 3, figsize = (20, 6))
    temp_plot = df_product_tpr[(df_product_tpr["CATEGORY"] == cate)]
    temp_plot = temp_plot[temp_plot["YEARWEEK"].between(202152, 202352)]
    sns.barplot(x = temp_plot["SPECIALEVENTNAME"], y = temp_plot["PRI_SALES_WITHOUT_BANDED"], ax=ax[0])
    sns.barplot(x = temp_plot["HOLIDAYNAME"], y = temp_plot["PRI_SALES_WITHOUT_BANDED"], ax=ax[1])
    sns.barplot(x = temp_plot["ABNORMAL"], y = temp_plot["PRI_SALES_WITHOUT_BANDED"], ax=ax[2])
    ax[0].set_title(cate)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Uplift vs number promotion, min_buy

# COMMAND ----------

df_agg_total = (
    df_product_tpr.groupby(["CATEGORY","YEARWEEK","UOM_PRODUCT"])[
        [
            "PRI_SALES_WITHOUT_BANDED",
            "PRI_BASELINE_WEEKLY",
            "PRI_SALES",
            "PROMOTION_count",
            "DEAL_TYPE_count_NON-U GIFT",
            "DEAL_TYPE_count_PERCENTAGE",
            "DEAL_TYPE_count_U GIFT",
            "DEAL_TYPE_count_VALUE",
            "MIN_BUY_mean_PERCENTAGE",
            "MIN_BUY_mean_NON-U GIFT",
            "MIN_BUY_mean_U GIFT",
            "MIN_BUY_mean_VALUE",
        ]
    ]
    .sum()
    .reset_index()
)
df_agg_total["DATE"] = pd.to_datetime(df_agg_total["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")
df_agg_total = df_agg_total[df_agg_total["YEARWEEK"] >= 202152]

df_agg_total["UPLIFT"] = (df_agg_total["PRI_SALES_WITHOUT_BANDED"] - df_agg_total["PRI_BASELINE_WEEKLY"]) / df_agg_total["PRI_BASELINE_WEEKLY"]
df_agg_total["UPLIFT"] = df_agg_total["UPLIFT"].replace([-np.inf, np.inf], 0).fillna(0)
df_agg_total["NEG_UPLIFT"] = df_agg_total[df_agg_total["UPLIFT"] < 0]["UPLIFT"].fillna(0)
df_agg_total["POS_UPLIFT"] = df_agg_total[df_agg_total["UPLIFT"] > 0]["UPLIFT"].fillna(0)

df_agg_total["UPLIFT_PERCENTAGE"] = df_agg_total[df_agg_total["DEAL_TYPE_count_PERCENTAGE"] > 0]["UPLIFT"].fillna(0)
df_agg_total["UPLIFT_VALUE"] = df_agg_total[df_agg_total["DEAL_TYPE_count_VALUE"] > 0]["UPLIFT"].fillna(0)
df_agg_total["UPLIFT_U GIFT"] = df_agg_total[df_agg_total["DEAL_TYPE_count_U GIFT"] > 0]["UPLIFT"].fillna(0)
df_agg_total["UPLIFT_NON-U GIFT"] = df_agg_total[df_agg_total["DEAL_TYPE_count_NON-U GIFT"] > 0]["UPLIFT"].fillna(0)


df_agg_total["MIN_BUY"] = df_agg_total[["MIN_BUY_mean_PERCENTAGE",
            "MIN_BUY_mean_NON-U GIFT",
            "MIN_BUY_mean_U GIFT",
            "MIN_BUY_mean_VALUE"
    ]
].mean(axis = 1)

df_agg_total["BIN"] = pd.qcut(df_agg_total["PROMOTION_count"], q = 10)
df_agg_total.groupby(["BIN"])[["UPLIFT","NEG_UPLIFT","POS_UPLIFT"]].sum().sort_index()

# COMMAND ----------

df_agg_total["BIN"] = df_agg_total["BIN"].astype(str)
display(df_agg_total)

# COMMAND ----------

df_agg_total["BIN"] = pd.qcut(df_agg_total["DEAL_TYPE_count_PERCENTAGE"], q = 10, duplicates="drop")
df_agg_total.groupby(["BIN"])[["UPLIFT","NEG_UPLIFT","POS_UPLIFT"]].sum().sort_index()

# COMMAND ----------

df_agg_total["BIN"] = pd.qcut(df_agg_total["MIN_BUY_mean_PERCENTAGE"], q = 10, duplicates="drop")
df_agg_total.groupby(["BIN"])[["UPLIFT","NEG_UPLIFT","POS_UPLIFT"]].sum().sort_index()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # pos uplift vs neg uplift weekly

# COMMAND ----------

df_eda = df_product_tpr.copy().sort_values(["CATEGORY","DPNAME","YEARWEEK"]).reset_index(drop = True)

df_eda["UPLIFT"] = (df_eda["PRI_SALES_WITHOUT_BANDED"] - df_eda["PRI_BASELINE_WEEKLY"]) / df_eda["PRI_BASELINE_WEEKLY"]
df_eda["UPLIFT"] = df_eda["UPLIFT"].replace([-np.inf, np.inf], 0).fillna(0)

avg_pos_to_neg = pd.DataFrame(columns = ["KEY","avg"])
for key, df_group in tqdm(df_eda.groupby(["CATEGORY","DPNAME"])):
    df_group = df_group.sort_values("YEARWEEK").reset_index(drop = True)
    key_df = key[0] + "|" + key[1]
    df_group["UPLIFT_NEXT_WEEK"] = df_group["UPLIFT"].shift(-1)

    count_week = 0
    list_count = []
    for i in range(df_group.shape[0]): 
        if (df_group.loc[i, "UPLIFT"] >= 0) & (df_group.loc[i, "UPLIFT_NEXT_WEEK"] >= 0):
            count_week += 1
        elif (df_group.loc[i, "UPLIFT"] > 0) & (df_group.loc[i, "UPLIFT_NEXT_WEEK"] < 0):
            list_count.append(count_week)
            count_week = 0
    avg_pos_to_neg = pd.concat([avg_pos_to_neg, pd.DataFrame({"KEY": key_df,"avg": np.mean(list_count)}, index = [0])])

avg_pos_to_neg[["CATEGORY","DPNAME"]] = avg_pos_to_neg["KEY"].str.split("|",expand = True)
display(avg_pos_to_neg.fillna(0))

# COMMAND ----------

df_eda_temp = pd.DataFrame(columns=["KEY","DIFF_1", "DIFF_2"])

for key, df_group in tqdm(df_eda[df_eda["YEARWEEK"].between(202152, 202352)].groupby(["CATEGORY", "DPNAME"])):
    df_group = df_group.sort_values("YEARWEEK").reset_index(drop=True)
    key_df = key[0] + "|" + key[1]

    for number_week in range(np.ceil(avg_pos_to_neg["avg"].median()).astype(int)):
        number_week += 1
        df_group[f"PROMOTION_count_diff{number_week}"] = df_group[
            (df_group["UPLIFT"] < 0)
            & (df_group["UPLIFT"].shift(1).rolling(number_week).sum() >= 0)
        ]["PROMOTION_count"].diff(number_week)
    df_eda_temp = pd.concat(
        [
            df_eda_temp,
            pd.DataFrame(
                {
                    "KEY": key_df,
                    "DIFF_1": df_group["PROMOTION_count_diff1"].mean(),
                    "DIFF_2": df_group["PROMOTION_count_diff2"].mean(),
                },
                index = [0]
            ),
        ]
    )

df_eda_temp[["CATEGORY","DPNAME"]] = df_eda_temp["KEY"].str.split("|",expand = True)
df_eda_temp = df_eda_temp.fillna(0)
display(df_eda_temp)

# COMMAND ----------

df_eda_temp[["DIFF_1","DIFF_2"]].describe()

# COMMAND ----------

