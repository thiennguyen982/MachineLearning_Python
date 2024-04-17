# Databricks notebook source
# MAGIC %sql
# MAGIC -- Timeseries: ds
# MAGIC -- Customer: Banner, region, 
# MAGIC -- Product: ulv_code
# MAGIC
# MAGIC with date_ranges as (
# MAGIC select
# MAGIC -- primary key
# MAGIC trim(lower(banner)) as banner,
# MAGIC region,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC min(to_date(billing_date)) as min_date,
# MAGIC max(to_date(billing_date)) as max_date
# MAGIC
# MAGIC from mt_predicted_order.mt_sale
# MAGIC group by banner, region, dp_name
# MAGIC )
# MAGIC , dates_expanded AS (
# MAGIC SELECT
# MAGIC EXPLODE(sequence(to_date("2023-01-01"), to_date("2024-03-01"), INTERVAL 1 DAY)) AS ds,
# MAGIC banner,
# MAGIC region,
# MAGIC dp_name
# MAGIC
# MAGIC FROM date_ranges
# MAGIC )
# MAGIC , dates_final as (
# MAGIC select
# MAGIC ds,
# MAGIC banner,
# MAGIC region,
# MAGIC dp_name
# MAGIC from dates_expanded
# MAGIC )
# MAGIC
# MAGIC -- Calendar
# MAGIC , calendar as (
# MAGIC select 
# MAGIC to_date(date) as ds,
# MAGIC year,
# MAGIC quarter,
# MAGIC month,
# MAGIC dayofmonth,
# MAGIC dayofweek,
# MAGIC weekend,
# MAGIC dayofyear,
# MAGIC weekofyear,
# MAGIC
# MAGIC is_year_start,
# MAGIC is_year_end,
# MAGIC is_quarter_start,
# MAGIC is_quarter_end,
# MAGIC is_month_start,
# MAGIC is_month_end,
# MAGIC
# MAGIC is_leap_year,
# MAGIC vn_holiday,
# MAGIC us_holiday,
# MAGIC
# MAGIC lunar_date,
# MAGIC lunar_year,
# MAGIC lunar_month,
# MAGIC lunar_day,
# MAGIC season
# MAGIC
# MAGIC from mt_predicted_order.mt_calendar_master
# MAGIC )
# MAGIC
# MAGIC , calendar_final as (
# MAGIC select *
# MAGIC from calendar
# MAGIC )
# MAGIC , product_information as (
# MAGIC select 
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC class, 
# MAGIC category,
# MAGIC packsize,
# MAGIC packgroup,
# MAGIC brand_variant,
# MAGIC subbrand,
# MAGIC brand,
# MAGIC segment,
# MAGIC format,
# MAGIC production_segment
# MAGIC from mt_predicted_order.mt_product_master
# MAGIC )
# MAGIC
# MAGIC , price_change as (
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
# MAGIC
# MAGIC -- promotion
# MAGIC , promotion_explode as (
# MAGIC select
# MAGIC explode(sequence(
# MAGIC     case when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_start_date) else null end,
# MAGIC     case when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_end_date) else null end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC
# MAGIC category,
# MAGIC post_name,
# MAGIC code_type,
# MAGIC on_off_post,
# MAGIC promotion_type,
# MAGIC promotion_grouping,
# MAGIC gift_type,
# MAGIC gift_value,
# MAGIC tts,
# MAGIC bmi,
# MAGIC actual_tts,
# MAGIC actual_bmi
# MAGIC
# MAGIC
# MAGIC from mt_predicted_order.mt_promotion
# MAGIC )
# MAGIC , promotion_final as (
# MAGIC select *
# MAGIC from promotion_explode
# MAGIC )
# MAGIC
# MAGIC -- sale
# MAGIC , sale as (
# MAGIC select 
# MAGIC -- primary key
# MAGIC to_date(billing_date) as ds,
# MAGIC lower(banner) as banner,
# MAGIC region,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC cast(est_demand_cs as bigint) as est_demand,
# MAGIC cast(order_cs as bigint) as order_cs,
# MAGIC cast(u_price_cs as bigint) as u_price_cs,
# MAGIC cast(key_in_after_value as bigint) as key_in_after_value
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
# MAGIC key_in_after_value
# MAGIC from sale
# MAGIC )
# MAGIC , final as (
# MAGIC select
# MAGIC -- primary key 
# MAGIC df.banner,
# MAGIC df.region,
# MAGIC df.dp_name,
# MAGIC concat_ws("_", year, month) as ds_freq,
# MAGIC
# MAGIC first(df.ds) as ds,
# MAGIC -- sale
# MAGIC avg(u_price_cs) as u_price_cs,
# MAGIC sum(order_cs) as order_cs,
# MAGIC sum(key_in_after_value) as key_in_after_value,
# MAGIC sum(est_demand) as est_demand,
# MAGIC
# MAGIC -- product information
# MAGIC first(class) as class,
# MAGIC first(brand) as brand,
# MAGIC first(pi.packsize) as packsize,
# MAGIC first(pi.packgroup) as packgroup,
# MAGIC first(pi.brand_variant) as brand_variant,
# MAGIC first(pi.category) as category,
# MAGIC first(pi.format) as format,
# MAGIC first(pi.segment) as segment,
# MAGIC first(pi.subbrand) as subbrand,
# MAGIC first(pi.production_segment) as production_segment,
# MAGIC
# MAGIC -- price change
# MAGIC first(relaunch) as relaunch,
# MAGIC first(activity_type) as activity_type,
# MAGIC first(price_change) as price_change,
# MAGIC
# MAGIC -- promotion
# MAGIC concat_ws(", ", collect_list(post_name)) as post_name,
# MAGIC concat_ws(", ", collect_list(code_type)) as code_type,
# MAGIC concat_ws(", ", collect_list(on_off_post)) as on_off_post,
# MAGIC concat_ws(", ", collect_list(promotion_type)) as promotion_type,
# MAGIC concat_ws(", ", collect_list(promotion_grouping)) as promotion_grouping,
# MAGIC concat_ws(", ", collect_list(gift_type)) as gift_type,
# MAGIC avg(gift_value) as gift_value,
# MAGIC avg(tts) as tts,
# MAGIC avg(bmi) as bmi,
# MAGIC avg(actual_tts) as actual_tts,
# MAGIC avg(actual_bmi) as actual_bmi,
# MAGIC
# MAGIC -- calendar
# MAGIC avg(year) as year,
# MAGIC avg(quarter) as quarter,
# MAGIC avg(month) as month,
# MAGIC avg(dayofmonth) as dayofmonth,
# MAGIC avg(dayofweek) as dayofweek,
# MAGIC first(weekend) as weekend,
# MAGIC avg(dayofyear) as dayofyear,
# MAGIC avg(weekofyear) as weekofyear,
# MAGIC
# MAGIC first(case when is_year_start = true then true else null end) as is_year_start,
# MAGIC first(case when is_year_end = true then true else null end) as is_year_end,
# MAGIC first(case when is_quarter_start = true then true else null end) as is_quarter_start,
# MAGIC first(case when is_quarter_end = true then true else null end) as is_quarter_end,
# MAGIC first(case when is_month_start = true then true else null end) as is_month_start,
# MAGIC first(case when is_month_end = true then true else null end) as is_month_end,
# MAGIC first(case when is_leap_year = true then true else null end) as is_leap_year,
# MAGIC
# MAGIC coalesce(any_value(case when vn_holiday != 'Non-holiday' then vn_holiday end), 'Non-holiday') as vn_holiday,
# MAGIC coalesce(any_value(case when us_holiday != 'Non-holiday' then us_holiday end), 'Non-holiday') as us_holiday,
# MAGIC
# MAGIC avg(lunar_year) as lunar_year,
# MAGIC avg(lunar_month) as lunar_month,
# MAGIC avg(lunar_day) as lunar_day,
# MAGIC first(season) as season
# MAGIC
# MAGIC
# MAGIC from dates_final df
# MAGIC left join sale_final sf on df.ds = sf.ds and df.region = sf.region and df.dp_name = sf.dp_name and df.banner = sf.banner
# MAGIC left join product_information pi on sf.ulv_code = pi.ulv_code
# MAGIC left join price_change_final pcf on df.ds = pcf.ds and df.banner = pcf.banner
# MAGIC left join promotion_final pf on df.ds = pf.ds and df.banner = pf.banner and df.dp_name = pf.dp_name
# MAGIC left join calendar_final cf on df.ds = cf.ds
# MAGIC where 1=1
# MAGIC group by 1, 2, 3, 4
# MAGIC )
# MAGIC , filter_top_sku as (
# MAGIC select
# MAGIC dp_name,
# MAGIC sum(est_demand) as sum_est_demand
# MAGIC from final 
# MAGIC group by 1
# MAGIC order by 2 desc 
# MAGIC limit 100
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC f.*
# MAGIC from final f
# MAGIC inner join filter_top_sku fts  on f.dp_name = fts.dp_name
# MAGIC where 1=1
# MAGIC and ds >= '2023-06-01'
# MAGIC and (banner = "saigon coop")
# MAGIC

# COMMAND ----------

_sqldf.cache().createOrReplaceTempView("data_mpo_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feature engineering
# MAGIC select 
# MAGIC ds_freq,
# MAGIC banner,
# MAGIC region,
# MAGIC dp_name,
# MAGIC
# MAGIC -- sale
# MAGIC u_price_cs,
# MAGIC order_cs,
# MAGIC key_in_after_value,
# MAGIC est_demand,
# MAGIC
# MAGIC -- product information
# MAGIC class,
# MAGIC brand,
# MAGIC packsize,
# MAGIC packgroup,
# MAGIC brand_variant,
# MAGIC category,
# MAGIC format,
# MAGIC segment,
# MAGIC subbrand,
# MAGIC production_segment,
# MAGIC
# MAGIC -- price change
# MAGIC relaunch,
# MAGIC activity_type,
# MAGIC price_change,
# MAGIC
# MAGIC -- promotion
# MAGIC post_name,
# MAGIC code_type,
# MAGIC on_off_post,
# MAGIC promotion_type,
# MAGIC promotion_grouping,
# MAGIC gift_type,
# MAGIC gift_value,
# MAGIC tts,
# MAGIC bmi,
# MAGIC actual_tts,
# MAGIC actual_bmi,
# MAGIC
# MAGIC -- calendar
# MAGIC year,
# MAGIC quarter,
# MAGIC month,
# MAGIC dayofmonth,
# MAGIC dayofweek,
# MAGIC weekend,
# MAGIC dayofyear,
# MAGIC weekofyear,
# MAGIC
# MAGIC is_year_start,
# MAGIC is_year_end,
# MAGIC is_quarter_start,
# MAGIC is_quarter_end,
# MAGIC is_month_start,
# MAGIC is_month_end,
# MAGIC
# MAGIC is_leap_year,
# MAGIC vn_holiday,
# MAGIC us_holiday,
# MAGIC
# MAGIC lunar_year,
# MAGIC lunar_month,
# MAGIC lunar_day,
# MAGIC season,
# MAGIC
# MAGIC
# MAGIC -- Feature Engineering
# MAGIC -- est demand
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 1 preceding and 1 preceding) as sum_est_demand_d1,
# MAGIC
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as sum_est_demand_d7,
# MAGIC avg(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as avg_est_demand_d7,
# MAGIC min(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as min_est_demand_d7,
# MAGIC max(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as max_est_demand_d7,
# MAGIC
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as sum_est_demand_d30,
# MAGIC avg(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as avg_est_demand_d30,
# MAGIC min(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as min_est_demand_d30,
# MAGIC max(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as max_est_demand_d30,
# MAGIC
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as sum_est_demand_d180,
# MAGIC avg(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as avg_est_demand_d180,
# MAGIC min(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as min_est_demand_d180,
# MAGIC max(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as max_est_demand_d180,
# MAGIC
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and 1 preceding) as sum_est_demand_d365,
# MAGIC avg(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and 1 preceding) as avg_est_demand_d365,
# MAGIC min(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and 1 preceding) as min_est_demand_d365,
# MAGIC max(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and 1 preceding) as max_est_demand_d365,
# MAGIC
# MAGIC ---- explanding est_demand
# MAGIC sum(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between unbounded preceding and 1 preceding) as expanding_sum_est_demand,
# MAGIC avg(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between unbounded preceding and 1 preceding) as expanding_avg_est_demand,
# MAGIC max(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between unbounded preceding and 1 preceding) as expanding_max_est_demand,
# MAGIC count(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between unbounded preceding and 1 preceding) as expanding_countnonzero_est_demand,
# MAGIC
# MAGIC -- promotion
# MAGIC -- -- post_name
# MAGIC count(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_count_post_name_d7,
# MAGIC
# MAGIC count(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_count_post_name_d30,
# MAGIC
# MAGIC count(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_count_post_name_d180,
# MAGIC -- -- tts
# MAGIC sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_sum_tts_d7,
# MAGIC avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_avg_tts_d7,
# MAGIC min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_min_tts_d7,
# MAGIC max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_max_tts_d7,
# MAGIC sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_count_tts_d7,
# MAGIC
# MAGIC sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_sum_tts_d30,
# MAGIC avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_avg_tts_d30,
# MAGIC min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_min_tts_d30,
# MAGIC max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_max_tts_d30,
# MAGIC sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_count_tts_d30,
# MAGIC
# MAGIC sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_sum_tts_d180,
# MAGIC avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_avg_tts_d180,
# MAGIC min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_min_tts_d180,
# MAGIC max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_max_tts_d180,
# MAGIC sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_count_tts_d180
# MAGIC from data_mpo_final dmf

# COMMAND ----------

_sqldf.write.mode("overwrite").saveAsTable("mt_feature_store.data_monthly_fe")

# COMMAND ----------

