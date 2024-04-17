# Databricks notebook source
# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

import pandas as pd
from lightgbm import LGBMClassifier

# COMMAND ----------

df = pd.read_csv('/Repos/ng-minh-hoang.dat@unilever.com/ds_projects/dt_snop/TPR/notebook/df_raw.csv')

# COMMAND ----------

df.head()

# COMMAND ----------

df.YEARWEEK.describe()

# COMMAND ----------

len(df.columns)

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.dtypes.value_counts()

# COMMAND ----------

df.CLASSIFY_UPLIFT.value_counts()

# COMMAND ----------

32535/(33163+32535)

# COMMAND ----------

# MAGIC %md
# MAGIC # Re-build model

# COMMAND ----------

# MAGIC %md
# MAGIC This is the LighGBM model copied from Dat. Keep everything the same for the post analysis purpose only

# COMMAND ----------

df_model = df.copy()

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

X_train.dtypes

# COMMAND ----------

f_importance = pd.DataFrame({'Feature_Name': X_train.columns, 'Weight_Importance': model.feature_importances_})

f_importance_sorted = f_importance.sort_values(by='Weight_Importance', ascending=False).reset_index(drop=True)
display(f_importance_sorted)

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # Check features

# COMMAND ----------

# MAGIC %md
# MAGIC ## DPName

# COMMAND ----------

def check_label_rate_by_grouping(df, column_name):
  check = df.groupby(column_name)["CLASSIFY_UPLIFT"].value_counts().unstack().reset_index().fillna(0)
  check["total"] = check[-1] + check[1]
  check["label_rate"] = check[1]/check["total"]
  check["distribution"] = check["total"]/check["total"].sum()
  return check


# COMMAND ----------

check = check_label_rate_by_grouping(df[df['YEARWEEK'] <= 202326], "DPNAME")

# COMMAND ----------

check

# COMMAND ----------

check = df[df["YEARWEEK"] <= 202326].groupby(["DPNAME", "CATEGORY"])["CLASSIFY_UPLIFT"].value_counts().unstack().reset_index().fillna(0)
check["total"] = check[-1] + check[1]
check["label_rate"] = check[1]/check["total"]
check["distribution"] = check["total"]/check["total"].sum()


# COMMAND ----------

check

# COMMAND ----------

check.to_excel("/Repos/ng-minh-hoang.dat@unilever.com/ds_projects/dt_snop/TPR/notebook/label_rate_by_dp_category.xlsx", index=False)

# COMMAND ----------

check.label_rate.describe(percentiles=[.25, .5, .75, .8, .85, .9, .95])

# COMMAND ----------

df[df["YEARWEEK"] <= 202326]["DPNAME"].value_counts().describe()

# COMMAND ----------

df.columns

# COMMAND ----------

check = check_label_rate_by_grouping(df[df["YEARWEEK"] <= 202326], "CATEGORY")

# COMMAND ----------

check

# COMMAND ----------

check.columns

# COMMAND ----------

import matplotlib.pyplot as plt
# Creating the subplots
fig, axs = plt.subplots(1, 2, figsize=(18, 6))

# Plotting the boxplot on the left
sns.boxplot(x='CATEGORY', y='label_rate', data=check, ax=axs[0], palette='pastel')
axs[0].set_title('label_rate by CATEGORY')

# Plotting the bar chart on the right
sns.barplot(x='distribution', y='CATEGORY', data=check, ax=axs[1], palette='pastel')
axs[1].set_title('distribution by CATEGORY')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Holiday name

# COMMAND ----------

df.head()

# COMMAND ----------

df.HOLIDAYNAME.value_counts()

# COMMAND ----------

df[df["YEARWEEK"] <= 202326].HOLIDAYNAME.value_counts()

# COMMAND ----------

data = df[df["YEARWEEK"] <= 202326]

# COMMAND ----------

check = check_label_rate_by_grouping(data, "HOLIDAYNAME")

# COMMAND ----------

check

# COMMAND ----------

# MAGIC %md
# MAGIC ## YEARWEEK

# COMMAND ----------

data.YEARWEEK.value_counts()

# COMMAND ----------

check = check_label_rate_by_grouping(data, "YEARWEEK")

# COMMAND ----------

check

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
plt.figure(figsize=(20,10))
sns.lineplot(x=check["YEARWEEK"].astype("str"), y="label_rate", data=check)
plt.xticks(rotation=70)
plt.show()

# COMMAND ----------

check = data.groupby(["YEARWEEK","WEEK_OF_YEAR", "WEEK_OF_QUARTER"])["CLASSIFY_UPLIFT"].value_counts().unstack().reset_index().fillna(0)
check["total"] = check[-1] + check[1]
check["label_rate"] = check[1]/check["total"]
check["distribution"] = check["total"]/check["total"].sum()

# COMMAND ----------

display(check)

# COMMAND ----------

check.to_excel("/Repos/ng-minh-hoang.dat@unilever.com/ds_projects/dt_snop/TPR/notebook/year_week_quarter.xlsx", index=False)

# COMMAND ----------

# Function to approximate month from week number
def week_to_month(week):
    if week <= 4:
        return 1  # January
    elif week <= 8:
        return 2  # February
    elif week <= 13:
        return 3  # March
    elif week <= 17:
        return 4  # April
    elif week <= 22:
        return 5  # May
    elif week <= 26:
        return 6  # June
    elif week <= 31:
        return 7  # July
    elif week <= 35:
        return 8  # August
    elif week <= 39:
        return 9  # September
    elif week <= 44:
        return 10 # October
    elif week <= 48:
        return 11 # November
    else:
        return 12 # December

# Function to determine quarter from week number
def week_to_quarter(week):
    if week <= 13:
        return 1
    elif week <= 26:
        return 2
    elif week <= 39:
        return 3
    else:
        return 4

# Apply the functions to create the month and quarter columns
check['month'] = check['WEEK_OF_YEAR'].apply(week_to_month)
check['quarter'] = check['WEEK_OF_YEAR'].apply(week_to_quarter)

# Display the first few rows to verify the new columns
check.head()


# COMMAND ----------

display(check)

# COMMAND ----------

# MAGIC %md
# MAGIC #sales features

# COMMAND ----------

data['PRI_BASELINE_GRP'] = pd.qcut(data['PRI_BASELINE'], 10)

# COMMAND ----------

data['PRI_BASELINE_GRP']

# COMMAND ----------

check = check_label_rate_by_grouping(data, "PRI_BASELINE_GRP")

# COMMAND ----------

data.head()

# COMMAND ----------

data.PRI_BASELINE_GRP.value_counts()

# COMMAND ----------

check = data.groupby("PRI_BASELINE_GRP")["CLASSIFY_UPLIFT"].value_counts().unstack().reset_index()

# COMMAND ----------

check["total"] = check[-1] + check[1]

# COMMAND ----------

check["label_rate"] = check[1]/check["total"]

# COMMAND ----------

check["distribution"] = check["total"]/check["total"].sum()

# COMMAND ----------

check

# COMMAND ----------

# MAGIC %md
# MAGIC ## uplift banded

# COMMAND ----------

data['UPLIFT_BANDED_GRP'] = pd.qcut(data['UPLIFT_BANDED'], 20, duplicates='drop')

# COMMAND ----------

data.UPLIFT_BANDED_GRP.value_counts()

# COMMAND ----------

import numpy as np
data['UPLIFT_BANDED_GRP'] = np.where(data["UPLIFT_BANDED"] > 0, ">0", "<=0")

# COMMAND ----------

check = check_label_rate_by_grouping(data, "UPLIFT_BANDED_GRP")

# COMMAND ----------

check

# COMMAND ----------

# MAGIC %md
# MAGIC # banded count

# COMMAND ----------

data.BANDED_COUNT.value_counts()

# COMMAND ----------

check = check_label_rate_by_grouping(data, "BANDED_COUNT")

# COMMAND ----------

check

# COMMAND ----------

