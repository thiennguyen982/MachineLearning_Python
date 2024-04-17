# Databricks notebook source
# MAGIC %sql
# MAGIC with price_change as (
# MAGIC select 
# MAGIC explode(sequence(
# MAGIC     case when to_date(start_date) <= to_date(end_date) then to_date(start_date) else null end,
# MAGIC     case when to_date(start_date) <= to_date(end_date) then to_date(end_date) else null end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC relaunch,
# MAGIC activity_type,
# MAGIC price_change
# MAGIC from mt_predicted_order.mt_price_change
# MAGIC )
# MAGIC
# MAGIC , price_change_final as (
# MAGIC select *
# MAGIC from price_change
# MAGIC )
# MAGIC -- sale
# MAGIC , sale as (
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
# MAGIC distinct
# MAGIC sf.*,
# MAGIC pcf.* except (ds, banner, dp_name)
# MAGIC from sale_final sf
# MAGIC inner join price_change_final pcf on sf.ds = pcf.ds and sf.banner = pcf.banner and sf.dp_name = pcf.dp_name

# COMMAND ----------

