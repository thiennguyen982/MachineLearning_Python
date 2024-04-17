# Databricks notebook source
import sys
import numpy as np
import pandas as pd
import pyspark.pandas as ps

sys.path.append("/Workspace/Repos/dao-minh.toan@unilever.com/ds_projects/")
from mt_predicted_order.common.sklearn import fa_avg_scorer, fa_sumup_scorer, fa_avg_score, fa_sumup_score


# COMMAND ----------

folder_name = "SGC-202331"
df_input = pd.read_excel(f"/dbfs/mnt/adls/YAFSU/{folder_name}/INPUT/EXCEL_XLSX/{folder_name.replace('-', '_')}.xlsx")
spark.createDataFrame(df_input).createOrReplaceTempView("sgc_input")

df_output = pd.read_parquet(f"/dbfs/mnt/adls/YAFSU/{folder_name}/OUTPUT/RESULT_nixtla_mlforecast_model_FC_EST_DEMAND.pkl.parquet")
spark.createDataFrame(df_output).createOrReplaceTempView("sgc_output")

folder_name = "BIG_C-202331"
df_input = pd.read_excel(f"/dbfs/mnt/adls/YAFSU/{folder_name}/INPUT/EXCEL_XLSX/{folder_name.replace('-', '_')}.xlsx")
spark.createDataFrame(df_input).createOrReplaceTempView("bigc_input")

df_output = pd.read_parquet(f"/dbfs/mnt/adls/YAFSU/{folder_name}/OUTPUT/RESULT_nixtla_mlforecast_model_FC_EST_DEMAND.pkl.parquet")
spark.createDataFrame(df_output).createOrReplaceTempView("bigc_output")

# COMMAND ----------

display(df_output)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from sgc_output
# MAGIC where 1=1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data and EDA

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with data_input as (
# MAGIC select 
# MAGIC *
# MAGIC from sgc_input
# MAGIC union all
# MAGIC select *
# MAGIC from bigc_input
# MAGIC
# MAGIC )
# MAGIC , data_output as (
# MAGIC select 
# MAGIC *
# MAGIC from sgc_output
# MAGIC union all
# MAGIC select *
# MAGIC from bigc_output
# MAGIC )
# MAGIC select 
# MAGIC concat_ws("|", banner, dp_name) as key_ts,
# MAGIC inp.yearweek,
# MAGIC inp.banner,
# MAGIC -- inp.region,
# MAGIC inp.dp_name,
# MAGIC est_demand,
# MAGIC out.LGBMRegressor,
# MAGIC out.NIXTLA_MLFC_EST_DEMAND_MEDIAN,
# MAGIC out.NIXTLA_MLFC_EST_DEMAND_MEAN,
# MAGIC abs(out.NIXTLA_MLFC_EST_DEMAND_MEDIAN - est_demand) as absolute_error
# MAGIC from data_input inp
# MAGIC left join data_output out on concat_ws("|", inp.banner, inp.dp_name) = out.key_ts and inp.yearweek = out.yearweek
# MAGIC where 1=1
# MAGIC and out.NIXTLA_MLFC_EST_DEMAND_MEAN is not null
# MAGIC order by yearweek
# MAGIC

# COMMAND ----------

folder_name = "SGC-202331"
df_input = pd.read_excel(f"/dbfs/mnt/adls/YAFSU/{folder_name}/INPUT/EXCEL_XLSX/{folder_name.replace('-', '_')}.xlsx")
spark.createDataFrame(df_input).createOrReplaceTempView("sgc_input")


# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from sgc_input
# MAGIC -- 31 lag 1, 2, 3, 4 
# MAGIC -- 

# COMMAND ----------

df_test = _sqldf.toPandas()
prediction = "NIXTLA_MLFC_EST_DEMAND_MEAN"
fa_avg = fa_avg_score(df_test['est_demand'], df_test[prediction])

fa_avg_df = pd.DataFrame({"FA_avg": [fa_avg]}, index = ["all"])

fa_avg_banner = df_test.groupby(['banner']).apply(
    lambda x: fa_avg_score(x['est_demand'], x[prediction])
).reset_index(name='fa_avg')

# fa_avg_banner_region = df_test.groupby(['banner', 'region']).apply(
#     lambda x: fa_avg_score(x['est_demand'], x[prediction])
# ).reset_index(name='fa_avg')


fa_avg_banner_region_dp_name = df_test.groupby(['banner', 'dp_name']).apply(
    lambda x: fa_avg_score(x['est_demand'], x[prediction])
).reset_index(name='fa_avg')

display(fa_avg_df)
display(fa_avg_banner)
# display(fa_avg_banner_region)
display(fa_avg_banner_region_dp_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC dhf.banner,
# MAGIC dhf.dp_name,
# MAGIC concat_ws('', cast(year as bigint), cast(weekofyear as bigint)) as yearweek,
# MAGIC sum(cast(dhf.est_demand as bigint)) as toan_est_demand,
# MAGIC sum(sgc.est_demand) as est_demand
# MAGIC from mt_feature_store.data_halfweek_fe dhf
# MAGIC inner join sgc_202344_input sgc on dhf.banner = trim(lower(sgc.banner)) and dhf.dp_name = sgc.dp_name and concat_ws('', cast(year as bigint), cast(weekofyear as bigint)) = yearweek
# MAGIC where 1=1
# MAGIC
# MAGIC group by 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC banner,
# MAGIC dp_name,
# MAGIC yearweek,
# MAGIC est_demand
# MAGIC from sgc_input
# MAGIC where 1=1
# MAGIC and dp_name = "lifebuoyhand wash total180g"
# MAGIC and yearweek = "202352"

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC banner,
# MAGIC dp_name,
# MAGIC year,
# MAGIC weekofyear,
# MAGIC est_demand
# MAGIC from mt_feature_store.data_halfweek_fe
# MAGIC where 1=1
# MAGIC and dp_name = "lifebuoyhand wash total180g"
# MAGIC -- and yearweek = "202352"

# COMMAND ----------

# MAGIC %sql
# MAGIC with sale as (
# MAGIC select 
# MAGIC -- primary key
# MAGIC to_date(billing_date) as ds,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(region)) as region,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC cast(est_demand_cs as bigint) as est_demand,
# MAGIC cast(order_cs as bigint) as order_cs,
# MAGIC cast(u_price_cs as bigint) as u_price_cs,
# MAGIC cast(key_in_after_value as bigint) as key_in_after_value,
# MAGIC cast(billing_cs as bigint) as billing_cs
# MAGIC from mt_predicted_order.mt_sale
# MAGIC )
# MAGIC
# MAGIC , sale_final as (
# MAGIC select
# MAGIC -- primary key 
# MAGIC ds,
# MAGIC banner,
# MAGIC region,
# MAGIC dp_name,
# MAGIC ulv_code,
# MAGIC coalesce(est_demand, 0) as est_demand,
# MAGIC order_cs,
# MAGIC u_price_cs,
# MAGIC key_in_after_value,
# MAGIC billing_cs as actual_sale
# MAGIC from sale
# MAGIC )
# MAGIC select 
# MAGIC ds,
# MAGIC banner,
# MAGIC dp_name,
# MAGIC est_demand
# MAGIC from sale_final
# MAGIC where 1=1
# MAGIC and banner = "saigon coop"
# MAGIC and dp_name like '%lifebuoyhand wash%180g%'
# MAGIC and ds >='2023-06-01'
# MAGIC order by ds

# COMMAND ----------

