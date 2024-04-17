# Databricks notebook source
# MAGIC %run "../../EnvironmentSetup"

# COMMAND ----------

# MAGIC %run "../YAFSU_Utils"

# COMMAND ----------

# MAGIC %run "../YAFSU_AutoForecast_Library"

# COMMAND ----------

dbutils.widgets.text('TARGET_VAR', '')
target_var = dbutils.widgets.get('TARGET_VAR')
if target_var == '':
    raise Exception("No Target Var - Revised Param")

# COMMAND ----------

import pandas as pd

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

df_AD = pd.read_parquet(
    "/dbfs/mnt/adls/SAP_HANA_DATASET/RAW_DATA/DT_DISTRIBUTOR_NONBANDED_YAFSU_INPUT.parquet"
)
display(df_AD)

# COMMAND ----------

df_Tam = pd.read_parquet("/dbfs/mnt/adls/Users/nguyen.hoang.tam/ssdt_202338.parquet")
display(df_Tam)

# COMMAND ----------

df_Tam["KEY_TS"] = df_Tam["DISTRIBUTOR"] + "|" + df_Tam["DP Name"]
df_Tam["DATE"] = pd.to_datetime(df_Tam["YEARWEEK"].astype(str) + "-4", format="%G%V-%w")
df_Tam["YEARWEEK"] = df_Tam["YEARWEEK"].astype(int)

# COMMAND ----------

from time import time 
round(time())

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j 
customLogs = log4jLogger.LogManager.getLogger("DT_DemandSensing_Logs") 

# COMMAND ----------

for target_var in [
    # "PCS_Friday",
    # "PCS_Monday",
    # "PCS_Saturday",
    # "PCS_Thursday",
    # "PCS_Tuesday",
    # "PCS_Wednesday",
    "week_cs",
    #"PCS_S1",
    #"PCS_S2",
]:
    for yearweek_cutoff in [*range(202325, 202338)]:
        start_time = time()

        df_cutoff = df_Tam.copy()
        df_cutoff["FUTURE"] = "N"
        df_cutoff["FUTURE"].loc[(df_cutoff["YEARWEEK"] > yearweek_cutoff)] = "Y"

        tasks = [
            REMOTE_nixtla_mlforecast_model.remote(
                key_ts,
                df_group,
                target_var=target_var,
                exo_vars=[],
                categorical_vars=[],
                horizon = 2,
            )
            for key_ts, df_group in df_cutoff.groupby("KEY_TS")
        ]

        output_data = ray.get(tasks)

        pkl.dump(
            output_data,
            #open(f"/dbfs/mnt/adls/YAFSU/DT_Demand_Sensing_Distributor/OUTPUT_NONBANDED_PKL/{target_var}_CUTOFF_{yearweek_cutoff}.pkl", "wb")
            open(f"/dbfs/mnt/adls/Users/nguyen.hoang.tam/YAFSU_OUTPUT_PKL/{target_var}_CUTOFF_{yearweek_cutoff}.pkl", "wb")
        )

        df_no_error_result = pd.concat([
            item['OUTPUT'] for item in output_data if item['ERROR'] == 'NO ERROR'
        ])

        #df_no_error_result.to_parquet(
        #    f"/dbfs/mnt/adls/YAFSU/DT_Demand_Sensing_Distributor/OUTPUT_NONBANDED_PARQUET/{target_var}/{target_var}_CUTOFF_{yearweek_cutoff}.parquet", index=False
        #)

        df_no_error_result.to_parquet(
            f"/dbfs/mnt/adls/Users/nguyen.hoang.tam/YAFSU_OUTPUT_PARQUET/{target_var}/{target_var}_CUTOFF_{yearweek_cutoff}.parquet", index=False
        )

        try:
            end_time = time() - start_time
            customLogs.info(f"Run for: {target_var} _ CUTOFF _ {yearweek_cutoff} takes {round(end_time, 2)} seconds")
        except:
            pass


# COMMAND ----------

