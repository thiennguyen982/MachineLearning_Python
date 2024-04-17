# Databricks notebook source
import os
import sys
import pyspark.pandas as ps

sys.path.append("/Workspace/Repos/dao-minh.toan@unilever.com/ds_projects/")
from mt_predicted_order.de.etl.utils import clean_column_name
# Sale
df_sale = ps.read_parquet("/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/LE_SEC_SALES_RAW_UPDATED_PARQUET")
df_sale.columns = [clean_column_name(col) for col in df_sale.columns]
(
    df_sale.to_spark()
    .write
    # .partitionBy("billing_date")
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dt_predicted_order.dt_sale")

)
display(df_sale)

# COMMAND ----------



# COMMAND ----------

# Promotion
df_promotion = ps.read_parquet("/mnt/adls/DT_PREDICTEDORDER/PROMOTION_TIMING")
df_promotion.columns = [clean_column_name(col) for col in df_promotion.columns]
(
    df_promotion.to_spark()
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dt_predicted_order.dt_promotion")
)

# COMMAND ----------

# product master

df_product: ps.DataFrame = ps.read_excel("/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALES_DEPENDENT_FILES/product_master.xlsx", header = 0, sheet_name = 0, dtype = "str")
df_product.columns = [clean_column_name(col) for col in df_product.columns]
# df_product["note"] = ""
df_product = df_product.dropna(axis = 1, how = "all")
(
    df_product.to_spark()
    .write
    .format("delta")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable("dt_predicted_order.dt_product", path = "/mnt/adls/dt_predicted_order/dt_product")
)


# COMMAND ----------

# Feature
import pyspark.pandas as ps
df_feature = ps.read_parquet("/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/DT_PROMOTION_FEATURES_DP_BANNER_YW.parquet")
display(df_feature)

# COMMAND ----------

df_product.to_spark().printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC with sale as (
# MAGIC select 
# MAGIC material as ulv_code,
# MAGIC banner,
# MAGIC distributor,
# MAGIC outlet_types,
# MAGIC sitecode_latest,
# MAGIC outletcode_latest,
# MAGIC to_date(updatedinvoicedate, 'yyyy-MM-dd') as ds, -- datetime
# MAGIC yearweek,
# MAGIC pcs  -- target: pcs(pieces)
# MAGIC
# MAGIC
# MAGIC from dt_predicted_order.dt_sale
# MAGIC )
# MAGIC , sale_final as (
# MAGIC select *
# MAGIC from sale
# MAGIC )
# MAGIC , promotion as (
# MAGIC select 
# MAGIC explode(sequence(
# MAGIC     case when to_date(start_promotion_of_week, 'yyyy-MM-dd') < to_date(end_promotion_of_week, 'yyyy-MM-dd') then to_date(start_promotion_of_week, 'yyyy-MM-dd') else to_date(end_promotion_of_week, 'yyyy-MM-dd') end,
# MAGIC     case when to_date(start_promotion_of_week, 'yyyy-MM-dd') < to_date(end_promotion_of_week, 'yyyy-MM-dd') then to_date(end_promotion_of_week, 'yyyy-MM-dd') else to_date(start_promotion_of_week, 'yyyy-MM-dd') end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC sku as ulv_code,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC trim(lower(banded)) as banded,
# MAGIC trim(lower(category)) as category,
# MAGIC trim(lower(promotion_type)) as promotion_type,
# MAGIC trim(lower(buy_type)) as buy_type,
# MAGIC trim(lower(discount_type)) as discount_type,
# MAGIC discount,
# MAGIC gift_type,
# MAGIC gift_quantity,
# MAGIC gift_uom,
# MAGIC duration
# MAGIC from dt_predicted_order.dt_promotion
# MAGIC )
# MAGIC , promotion_final as (
# MAGIC select *
# MAGIC from promotion
# MAGIC )
# MAGIC , product_information as (
# MAGIC select 
# MAGIC ulv_code,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC division,
# MAGIC brand,
# MAGIC pack_size,
# MAGIC sale_org,
# MAGIC pcs_cs,
# MAGIC category,
# MAGIC code_type,
# MAGIC product_classification
# MAGIC from dt_predicted_order.dt_product
# MAGIC
# MAGIC )
# MAGIC select 
# MAGIC sf.*,
# MAGIC pi.* except(ulv_code),
# MAGIC pf.* except(ds, ulv_code, banner),
# MAGIC cast(pcs as bigint)/cast(pcs_cs as bigint) as cs
# MAGIC from sale_final sf 
# MAGIC left join product_information pi on sf.ulv_code =  pi.ulv_code
# MAGIC left join promotion_final pf on sf.ds = pf.ds and sf.ulv_code = pf.ulv_code and sf.banner = pf.banner 
# MAGIC -- weekly, key forecast: dp_name, distributor, label: cs lag 1, 2, 3, 4
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with  promotion as (
# MAGIC select 
# MAGIC promotion_id,
# MAGIC explode(sequence(
# MAGIC     case when to_date(start_promotion_of_week, 'yyyy-MM-dd') < to_date(end_promotion_of_week, 'yyyy-MM-dd') then to_date(start_promotion_of_week, 'yyyy-MM-dd') else to_date(end_promotion_of_week, 'yyyy-MM-dd') end,
# MAGIC     case when to_date(start_promotion_of_week, 'yyyy-MM-dd') < to_date(end_promotion_of_week, 'yyyy-MM-dd') then to_date(end_promotion_of_week, 'yyyy-MM-dd') else to_date(start_promotion_of_week, 'yyyy-MM-dd') end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC sku as ulv_code,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC trim(lower(banded)) as banded,
# MAGIC trim(lower(category)) as category,
# MAGIC trim(lower(promotion_type)) as promotion_type,
# MAGIC trim(lower(buy_type)) as buy_type,
# MAGIC trim(lower(discount_type)) as discount_type,
# MAGIC discount,
# MAGIC gift_type,
# MAGIC gift_quantity,
# MAGIC gift_uom,
# MAGIC duration
# MAGIC from dt_predicted_order.dt_promotion
# MAGIC )
# MAGIC , promotion_final as (
# MAGIC select *
# MAGIC from promotion
# MAGIC )
# MAGIC select 
# MAGIC promotion_id,
# MAGIC ds,
# MAGIC ulv_code,
# MAGIC count(*),
# MAGIC concat(collect_list(discount)) as concat_discount
# MAGIC from promotion_final
# MAGIC where 1=1
# MAGIC and discount_type  = "percentage"
# MAGIC group by 1, 2, 3
# MAGIC order by 4 desc
# MAGIC -- select 
# MAGIC -- distinct
# MAGIC -- promotion_type
# MAGIC -- from promotion_final
# MAGIC -- where 1=1
# MAGIC -- and promotion_type  = "line discount"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with  promotion as (
# MAGIC select 
# MAGIC promotion_id,
# MAGIC explode(sequence(
# MAGIC     case when to_date(start_promotion_of_week, 'yyyy-MM-dd') < to_date(end_promotion_of_week, 'yyyy-MM-dd') then to_date(start_promotion_of_week, 'yyyy-MM-dd') else to_date(end_promotion_of_week, 'yyyy-MM-dd') end,
# MAGIC     case when to_date(start_promotion_of_week, 'yyyy-MM-dd') < to_date(end_promotion_of_week, 'yyyy-MM-dd') then to_date(end_promotion_of_week, 'yyyy-MM-dd') else to_date(start_promotion_of_week, 'yyyy-MM-dd') end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC sku as ulv_code,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC trim(lower(banded)) as banded,
# MAGIC trim(lower(category)) as category,
# MAGIC trim(lower(promotion_type)) as promotion_type,
# MAGIC trim(lower(buy_type)) as buy_type,
# MAGIC trim(lower(discount_type)) as discount_type,
# MAGIC discount,
# MAGIC gift_type,
# MAGIC gift_quantity,
# MAGIC gift_uom,
# MAGIC duration
# MAGIC from dt_predicted_order.dt_promotion
# MAGIC )
# MAGIC , promotion_final as (
# MAGIC select *
# MAGIC from promotion
# MAGIC )

# COMMAND ----------

dbutils.fs.rm("/mnt/adls/dt_predicted_order/dt_product", True)

# COMMAND ----------

