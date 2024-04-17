# Databricks notebook source
# MAGIC %pip install --upgrade numpy
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import explode, split, col, dayofweek, to_date, when, concat, lit
import pyspark.pandas as ps_pd
import pandas as pd

# COMMAND ----------

# MAGIC %sh
# MAGIC "/Workspace/Repos/nguyen-hoang.tam@unilever.com/ds_projects/demand_sensing_yafsu_run/dev/APPLICATION_YAFSU/dt_snop.sh"
# MAGIC

# COMMAND ----------

# %sh
# "/Workspace/Users/nguyen-hoang.tam@unilever.com/(Clone) Application YAFSU (Tam)/dt_snop.sh"

# COMMAND ----------

# MAGIC %run "../../EnvironmentSetup"

# COMMAND ----------

# MAGIC %run "../YAFSU_Utils"

# COMMAND ----------

# MAGIC %run "../YAFSU_AutoForecast_Library"

# COMMAND ----------

@ray.remote
def REMOTE_nixtla_mlforecast_model(
    key_ts, df_group, target_var, exo_vars, categorical_vars, horizon
):
    return nixtla_mlforecast_model(
        key_ts=key_ts,
        df_group=df_group,
        target_var=target_var,
        exo_vars=exo_vars,
        categorical_vars=categorical_vars,
        horizon=horizon,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # LOAD SECOND SALES DATA

# COMMAND ----------

link = "/dbfs/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/LE_SEC_SALES_YAFSU_FORMAT_PARQUET"
df_input_total = pd.read_parquet(link)
df_input_non_banded = df_input_total[df_input_total["Banded"] == "No"]

# COMMAND ----------

# MAGIC %md
# MAGIC # AGG SALES TO DP|DIST LEVEL

# COMMAND ----------

df_input_cs_week = df_input_non_banded.groupby(["DISTRIBUTOR", "DPNAME", "YEARWEEK", "BANNER"]).agg(CS_WEEK = ("CS_WEEK", "sum")).reset_index() #change the dataset to get nonbanded only or total
df_input_cs_week["YEARWEEK"] = df_input_cs_week["YEARWEEK"].astype(int)

# display(df_input_cs_week)

# COMMAND ----------

# display(df_input_non_banded[["Banded"]].drop_duplicates())

# COMMAND ----------

# display(df_input_cs_week.query("DISTRIBUTOR == '15085676'").groupby(["BANNER", "DPNAME", "YEARWEEK"]).agg(CS_WEEK = ("CS_WEEK", "sum")).reset_index().query("DPNAME == 'CLEAR SHAMPOO MENTHOL 650G' & YEARWEEK == 202336"))




# COMMAND ----------

# MAGIC %md
# MAGIC # LOAD FEATURE DATA

# COMMAND ----------

feature_link = "/dbfs/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/DT_PROMOTION_FEATURES_DP_BANNER_YW.parquet"
df_features = pd.read_parquet(feature_link)
df_features["YEARWEEK"] = df_features["YEARWEEK"].astype(int)
# display(df_features)

# COMMAND ----------

# display(df_features.query("DPNAME == 'POND PINKISH SL CREAM 50G' & (YEARWEEK == 202342 | YEARWEEK == 202343  | YEARWEEK == 202344) & BANNER == 'DT HCME'"))



# COMMAND ----------

# MAGIC %md
# MAGIC # JOIN THE TABLES

# COMMAND ----------

df_input = df_input_cs_week.merge(df_features, on = ["YEARWEEK", "DPNAME", "BANNER"], how = "outer").fillna(0)
df_input["KEY_TS"] = df_input["DISTRIBUTOR"].astype(str) +  "|"  + df_input["DPNAME"]
df_input["DATE"] = pd.to_datetime(df_input["YEARWEEK"].astype(str) + "-4", format="%G%V-%w")
df_input["YEARWEEK"] = df_input["YEARWEEK"].astype(int)

# COMMAND ----------

# display(df_input)

# COMMAND ----------

# MAGIC %md
# MAGIC # YAFSU PARAMETER SET-UP

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXO VAR SET UP 

# COMMAND ----------

'''
Set exo_vars to [] when run without promotion
'''


exo_vars =  ["NON-U GIFT", "PERCENTAGE", "U GIFT", "VALUE", "NUMBER_OF_PROMO", "Basket", "Bonus Buy Free", "Line Discount", "Set Discount", "FIRST_WEEK", "LAST_WEEK", "MID_WEEK", "HOLIDAY_WEEK", "NON_HOLIDAY_WEEK", "BUY_VALUE", "BUY_VOLUME", "min(Value Discount)", "max(Value Discount)", "avg(Value Discount)", "min(% Discount)", "max(% Discount)", "avg(% Discount)"]

#check exo_vars names are correct:

for exo_var in exo_vars:
    if exo_var not in df_input.columns:
        raise Exception(f"Hey, {exo_var} is incorrect ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TARGET LIST

# COMMAND ----------

target_list = ["CS_WEEK",
    # "CS_HW1",
    # "CS_HW2",
    # "CS_Monday",
    # "CS_Saturday",
    # "CS_Thursday",
    # "CS_Wednesday",
    # "CS_Tuesday",
    # "CS_Friday"
    ]
for target_var in target_list:
  if target_var not in list(df_input.columns):
    raise Exception(f"Hey, check {target_var} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## YEARWEEK CUTOFF

# COMMAND ----------

yw_cutoff_range = [*range(202333, 202349)]
print(yw_cutoff_range)

# COMMAND ----------

# MAGIC %md
# MAGIC ## HORIZON

# COMMAND ----------

'''
README: horizon (LAG_FC) won't be necessarily the number you set it to be, it also depends on the last sales week of the time series and how many FUTURE == Y records that time series has. For example, if the LSW == YW_CUTOFF, LAG_FC = horizon, else LAG_FC = len(TS.where(FUTURE = Y))
'''
horizon = 3


# COMMAND ----------

# MAGIC %md
# MAGIC ## RUN AT

# COMMAND ----------

from datetime import date
import os
run_on = int(date.today().strftime("%Y%m%d")) 
print(run_on)

# COMMAND ----------

# MAGIC %md
# MAGIC # YAFSU RUN

# COMMAND ----------

df = df_input.copy()
for target_var in target_list:
    os.makedirs(
    name=f"/dbfs/mnt/adls/DT_PREDICTEDORDER/TESTING_OUTPUTS/NON_BANDED_WITH_PROMO/{target_var}",
    exist_ok=True,
)
    for yearweek_cutoff in yw_cutoff_range:
        start_time = time()

        df_cutoff = df.copy()
        df_cutoff["FUTURE"] = "N"
        df_cutoff["FUTURE"].loc[(df_cutoff["YEARWEEK"] > yearweek_cutoff)] = "Y"

        tasks = [
            REMOTE_nixtla_mlforecast_model.remote(
                key_ts,
                df_group,
                target_var=target_var,
                exo_vars = exo_vars, #WITH PROMOTION 
                categorical_vars=[],
                horizon= horizon, 
            )
            for key_ts, df_group in df_cutoff.groupby("KEY_TS")
        ]

        #1
        print("You are here #1")

        output_data = ray.get(tasks)

        pkl.dump(
            output_data,
            open(f"/dbfs/mnt/adls/DT_PREDICTEDORDER/TESTING_OUTPUTS/NON_BANDED_WITH_PROMO/{target_var}/{run_on}_LE_DTSS_{target_var}_CUTOFF_{yearweek_cutoff}.pkl", "wb")
        )
        #2

        print("You are here #2")

        df_no_error_result = pd.concat([
            item['OUTPUT'] for item in output_data if item['ERROR'] == 'NO ERROR'
        ])

        df_no_error_result.to_parquet(
            f"/dbfs/mnt/adls/DT_PREDICTEDORDER/TESTING_OUTPUTS/NON_BANDED_WITH_PROMO/{target_var}/{run_on}_LE_DTSS_{target_var}_CUTOFF_{yearweek_cutoff}.parquet", index=False
        )

        # df_error_result = pd.concat([
        #     item['OUTPUT'] for item in output_data if item['ERROR'] != 'NO ERROR'
        # ])

        # df_error_result.to_parquet(
        #     f"/dbfs/mnt/adls/Users/nguyen.hoang.tam/dfSS_Q3_{target_var}/{target_var}_CUTOFF_{yearweek_cutoff}_DIST_DPNAME_error.parquet", index=False
        # )

        print(f"Finished {target_var}_{yearweek_cutoff}")

        # print(df_error_result)

        try:
            end_time = time() - start_time
            customLogs.info(f"Run for: {target_var} _ CUTOFF _ {yearweek_cutoff} takes {round(end_time, 2)} seconds")
        except:
            pass

# COMMAND ----------

