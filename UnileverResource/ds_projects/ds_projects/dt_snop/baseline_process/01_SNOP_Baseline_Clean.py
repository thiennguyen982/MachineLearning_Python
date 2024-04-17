# Databricks notebook source
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

df_promo_classifier = pd.read_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_PROMO_CLASSIFICATION.parquet"
)
df_promo_classifier["GREEN"] = np.where(
  np.logical_or(df_promo_classifier["BIZ_FINAL_ACTIVITY_TYPE"] == "GREEN", df_promo_classifier["ABNORMAL"] == "Y"), 1, 0
)
df_promo_classifier = df_promo_classifier.rename(columns={"REGION": "BANNER"})
df_promo_classifier = df_promo_classifier[["BANNER", "DPNAME", "YEARWEEK", "GREEN"]]

# COMMAND ----------

# There was an issue that some category have sec sales gap more than raw sec data from HANA (especially IC category)
# Dat found out that df_promo_classifier was update and duplicate -> DF either duplicated  when merge
print(df_promo_classifier[df_promo_classifier.duplicated()].shape)
df_promo_classifier = df_promo_classifier.drop_duplicates()

# COMMAND ----------

DF = pd.read_parquet(
    # "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_SEC_SALES_NONB2B_AGG.parquet"
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_SEC_SALES_TOTAL_DT_AGG.parquet"
)
DF = DF.rename(columns={"REGION": "BANNER"})

# COMMAND ----------

df_calendar_workingday = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx",
    sheet_name="Week Master",
    dtype=object,
)

df_calendar_workingday = df_calendar_workingday[["Week.Year", "CD working day"]]
df_calendar_workingday["Week.Year"] = df_calendar_workingday["Week.Year"].astype(str)
yearweek = df_calendar_workingday["Week.Year"].str.split(".", expand=True)
df_calendar_workingday["YEARWEEK"] = yearweek[1].astype(int) * 100 + yearweek[0].astype(
    int
)
df_calendar_workingday = df_calendar_workingday[["YEARWEEK", "CD working day"]]
df_calendar_workingday.columns = ["YEARWEEK", "DTWORKINGDAY"]

DF = DF.merge(df_calendar_workingday, on="YEARWEEK")

# COMMAND ----------

DF["DAILY_AVG_SALES"] = DF["ACTUALSALE"].div(DF["DTWORKINGDAY"].astype(float))
DF["DAILY_AVG_SALES"].loc[(DF["DTWORKINGDAY"] == 0)] = 0

# COMMAND ----------

DF = DF.sort_values(by=["BANNER", "CATEGORY", "DPNAME", "YEARWEEK"])
DF["KEY"] = DF["BANNER"] + "|" + DF["CATEGORY"] + "|" + DF["DPNAME"]
# DF['KEY_PROMO'] = DF['BANNER'] + "|" + DF['DPNAME']

# COMMAND ----------

DF = DF.merge(df_promo_classifier, how="left", on=["BANNER", "DPNAME", "YEARWEEK"])
DF["GREEN"] = DF["GREEN"].fillna(0)

# COMMAND ----------

df_promo_nationwide = DF.groupby(["DPNAME", "YEARWEEK"])["GREEN"].max().reset_index()
df_promo_nationwide = df_promo_nationwide.rename(columns={"GREEN": "GREEN_NW"})
DF = DF.merge(df_promo_nationwide, on=["DPNAME", "YEARWEEK"], how="left")
DF["GREEN_NW"] = DF["GREEN_NW"].fillna(0)

# COMMAND ----------

DF["GREEN"] = DF[["GREEN", "GREEN_NW"]].max(axis=1)

# COMMAND ----------

display(DF)

# COMMAND ----------

from scipy import stats


def baseline_clean(key, df_group):
    window = 13
    df_group["DAILY_AVG_SALES_ORIGINAL"] = df_group["DAILY_AVG_SALES"]

    # Clean at first sight with +/- 1.5*STD
    df_group_yellow = df_group.query("GREEN == 0")
    total_mean = df_group_yellow["DAILY_AVG_SALES"].mean()
    total_std = df_group_yellow["DAILY_AVG_SALES"].std()
    total_range_up = total_mean + 1 * total_std
    total_range_down = total_mean - 1 * total_std

    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] > total_range_up)
    ] = total_range_up
    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] < total_range_down)
    ] = total_range_down

    # Clean with moving aggregations features
    moving_average = df_group["DAILY_AVG_SALES"].rolling(window).mean()
    moving_std = df_group["DAILY_AVG_SALES"].rolling(window).std()
    moving_median = df_group["DAILY_AVG_SALES"].rolling(window).median()
    moving_median_abs_deviation = (
        df_group["DAILY_AVG_SALES"].rolling(window).apply(stats.median_abs_deviation)
    )

    df_group["MA"] = moving_average
    df_group["MSTD"] = moving_std
    df_group["MM"] = moving_median
    df_group["MMAD"] = moving_median_abs_deviation

    df_group["RANGE_UP_MASTD"] = df_group["MA"] + df_group["MSTD"]
    df_group["RANGE_DOWN_MASTD"] = df_group["MA"] - df_group["MSTD"]
    df_group["RANGE_UP_MEDIAN"] = df_group["MM"] + 2 * df_group["MMAD"]
    df_group["RANGE_DOWN_MEDIAN"] = df_group["MM"] - 2 * df_group["MMAD"]

    # df_group['RANGE_UP'] = df_group[['RANGE_UP_MEDIAN', 'RANGE_UP_MASTD']].min(axis=1)
    # df_group['RANGE_DOWN'] = df_group[['RANGE_DOWN_MEDIAN', 'RANGE_DOWN_MASTD']].max(axis=1)

    df_group["RANGE_UP"] = df_group["RANGE_UP_MASTD"]
    df_group["RANGE_DOWN"] = df_group["RANGE_DOWN_MASTD"]

    df_group["BASELINE"] = df_group["DAILY_AVG_SALES"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP"])
    ] = df_group["RANGE_UP"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN"])
    ] = df_group["RANGE_DOWN"]
    df_group["BASELINE_DAILY"] = df_group["BASELINE"]
    df_group["BASELINE"] = df_group["BASELINE"] * df_group["DTWORKINGDAY"]

    df_group["KEY"] = key

    return df_group


def baseline_clean_52weeks(key, df_group):
    window = 13
    df_group["DAILY_AVG_SALES_ORIGINAL"] = df_group["DAILY_AVG_SALES"]
    std_ratio = 1

    df_group = df_group.sort_values("YEARWEEK")
    # Clean at first sight with 52-Weeks Yellow
    moving_average = (
        df_group["DAILY_AVG_SALES"]
        .rolling(52)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .mean()
        )
    )
    moving_std = (
        df_group["DAILY_AVG_SALES"]
        .rolling(52)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .std()
        )
    )
    df_group["MA_YELLOW"] = moving_average
    df_group["MSTD_YELLOW"] = moving_std
    df_group["RANGE_UP"] = df_group["MA_YELLOW"] + std_ratio * df_group["MSTD_YELLOW"]
    df_group["RANGE_DOWN"] = df_group["MA_YELLOW"] - std_ratio * df_group["MSTD_YELLOW"]

    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP"])
    ] = df_group["RANGE_UP"]
    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN"])
    ] = df_group["RANGE_DOWN"]

    # Clean with moving aggregations features
    moving_average = df_group["DAILY_AVG_SALES"].rolling(window).mean()
    moving_std = df_group["DAILY_AVG_SALES"].rolling(window).std()
    # moving_median = df_group['DAILY_AVG_SALES'].rolling(window).median()
    # moving_median_abs_deviation = df_group['DAILY_AVG_SALES'].rolling(window).apply(stats.median_abs_deviation)

    df_group["MA"] = moving_average
    df_group["MSTD"] = moving_std
    # df_group['MM'] = moving_median
    # df_group['MMAD'] = moving_median_abs_deviation

    # df_group['RANGE_UP_MASTD'] = df_group['MA'] + df_group['MSTD']
    # df_group['RANGE_DOWN_MASTD'] = df_group['MA'] - df_group['MSTD']
    # df_group['RANGE_UP_MEDIAN'] = df_group['MM'] + 2*df_group['MMAD']
    # df_group['RANGE_DOWN_MEDIAN'] = df_group['MM'] - 2*df_group['MMAD']

    # df_group['RANGE_UP'] = df_group[['RANGE_UP_MEDIAN', 'RANGE_UP_MASTD']].min(axis=1)
    # df_group['RANGE_DOWN'] = df_group[['RANGE_DOWN_MEDIAN', 'RANGE_DOWN_MASTD']].max(axis=1)

    df_group["RANGE_UP"] = df_group["MA"] + std_ratio * df_group["MSTD"]
    df_group["RANGE_DOWN"] = df_group["MA"] - std_ratio * df_group["MSTD"]

    df_group["BASELINE"] = df_group["DAILY_AVG_SALES"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP"])
    ] = df_group["RANGE_UP"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN"])
    ] = df_group["RANGE_DOWN"]
    df_group["BASELINE_DAILY"] = df_group["BASELINE"]
    df_group["BASELINE"] = df_group["BASELINE"] * df_group["DTWORKINGDAY"]

    df_group["KEY"] = key

    return df_group

# COMMAND ----------

def baseline_clean_20230524(key, df_group):
    df_group["DAILY_AVG_SALES_ORIGINAL"] = df_group["DAILY_AVG_SALES"]
    std_ratio = 1

    df_group = df_group.sort_values("YEARWEEK")

    ######### Stage 01

    # Calculate moving average backward / forward 26 weeks.

    moving_average_left = (
        df_group["DAILY_AVG_SALES"]
        .rolling(26)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .mean()
        )
    )

    moving_average_right = (
        df_group["DAILY_AVG_SALES"]
        .iloc[::-1]
        .rolling(26)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .mean()
        )
        .iloc[::-1]
    )

    moving_average = moving_average_left + moving_average_right
    moving_average = moving_average / 2
    moving_average = moving_average.fillna(moving_average_left).fillna(
        moving_average_right
    )

    # Calculate moving STD backward 26 weeks.
    moving_std = (
        df_group["DAILY_AVG_SALES"]
        .rolling(26)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .std()
        )
    )

    # Cut outlier 1st Stage

    df_group["MA_YELLOW"] = moving_average
    df_group["MSTD_YELLOW"] = moving_std
    df_group["RANGE_UP_YELLOW"] = (
        df_group["MA_YELLOW"] + std_ratio * df_group["MSTD_YELLOW"]
    )
    df_group["RANGE_DOWN_YELLOW"] = (
        df_group["MA_YELLOW"] - std_ratio * df_group["MSTD_YELLOW"]
    )

    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP_YELLOW"])
    ] = df_group["RANGE_UP_YELLOW"]
    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN_YELLOW"])
    ] = df_group["RANGE_DOWN_YELLOW"]

    ######### Stage 02

    moving_average_left = df_group["DAILY_AVG_SALES"].shift(1).rolling(6).mean()
    moving_average_right = (
        df_group["DAILY_AVG_SALES"].shift(-1).iloc[::-1].rolling(6).mean().iloc[::-1]
    )
    moving_average = moving_average_left + moving_average_right
    moving_average = moving_average / 2
    moving_average = moving_average.fillna(moving_average_left).fillna(
        moving_average_right
    )

    moving_std_left = df_group["DAILY_AVG_SALES"].shift(1).rolling(6).std()
    moving_std_right = (
        df_group["DAILY_AVG_SALES"].shift(-1).iloc[::-1].rolling(6).std().iloc[::-1]
    )
    moving_std = moving_std_left + moving_std_right
    moving_std = moving_std / 2
    moving_std = moving_std.fillna(moving_std_left).fillna(moving_std_right)

    df_group["MA"] = moving_average
    df_group["MSTD"] = moving_std

    df_group["RANGE_UP"] = df_group["MA"] + std_ratio * df_group["MSTD"]
    df_group["RANGE_DOWN"] = df_group["MA"] - std_ratio * df_group["MSTD"]

    df_group["BASELINE"] = df_group["DAILY_AVG_SALES"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP"])
    ] = df_group["RANGE_UP"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN"])
    ] = df_group["RANGE_DOWN"]
    df_group["BASELINE_DAILY"] = df_group["BASELINE"]
    df_group["BASELINE"] = df_group["BASELINE"] * df_group["DTWORKINGDAY"]

    df_group["KEY"] = key

    return df_group

# COMMAND ----------

@ray.remote
def REMOTE_baseline_clean(key, df_group):
    warnings.filterwarnings("ignore")
    df_result = baseline_clean_20230524(key, df_group)
    print(key)
    # df_result = baseline_clean(key, df_group)
    # df_result = baseline_clean_52weeks(key, df_group)
    return df_result

# COMMAND ----------

tasks = [
    REMOTE_baseline_clean.remote(key, df_group) for key, df_group in DF.groupby("KEY")
]
tasks = ray.get(tasks)
DF_ALL = pd.concat(tasks)

# COMMAND ----------

DF_ALL.to_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE.parquet", index=False
)

# COMMAND ----------

# for key, df_group in DF_ALL.groupby('CATEGORY'):
#   print (key)
#   write_excel_file(df=df_group, file_name=key, sheet_name='DATA', dbfs_directory='/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE_FOLDER')

# COMMAND ----------

# DF_ALL.query("YEARWEEK > 202200 & YEARWEEK < 202253 & CATEGORY == 'FABSOL'")['ACTUALSALE'].sum()

# COMMAND ----------

display(DF_ALL[(DF_ALL['DPNAME'] == 'OMO LIQUID MATIC FL EXPERT (POU) 3.7KG')])

# COMMAND ----------

