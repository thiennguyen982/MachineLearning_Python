# Databricks notebook source
# MAGIC %md
# MAGIC # Import packages

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame as PySparkDataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup datetime

# COMMAND ----------

curr_month_str = datetime.today().replace(day=1).strftime('%Y-%m-%d')
pre_month_str = (datetime.today().replace(day=1) - relativedelta(months=1)).strftime('%Y-%m-%d')
last_12_month_str = (datetime.today().replace(day=1) - relativedelta(months=12)).strftime('%Y-%m-%d')
current_year = datetime.today().year
print(f"Current month: {curr_month_str}")
print(f"Pre month: {pre_month_str}")
print(f"Last 12 months: {last_12_month_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read VN global product master

# COMMAND ----------

def load_vn_global_product_master_raw() -> PySparkDataFrame:
  '''This function is to load the product master of VN raw. Which means loading everything from the source and filter
  Vietnam only. This is the mapping from GLOBAL
  Arguments: None
  Return: Pyspark dataframe already filtered of VN only
  '''
  product_master_filepath = '/mnt/adls/UDL_Gen2/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/ProductMaster/SouthEastAsiaAustralasia/Processed/'
  df_product_master = spark.read.format("csv").options(delimiter="|", header="True").load(product_master_filepath)
  df_product_master = df_product_master.filter(F.col("COUNTRY")=="VN")
  return df_product_master

# COMMAND ----------

def rename_columns_product_master(df: PySparkDataFrame) -> PySparkDataFrame:
    """
    Substring the 'MATERIAL' column to extract the rightmost 8 characters and rename specific columns.

    Arguments:
    df (PySparkDataFrame): A PySpark DataFrame containing the data to be processed. This DataFrame should
        include a 'MATERIAL' column that will be substringed to extract the rightmost 8 characters.
        
    Returns:
    PySparkDataFrame: A PySpark DataFrame with the 'MATERIAL' column substringed and renamed columns.
    """
    result = df.withColumn("product_code", F.expr("substr(MATERIAL, -8, 8)"))
    result = result.withColumnRenamed("/BIC/ZPA_PAPH2", "PH2")
    result = result.withColumnRenamed("/BIC/ZPA_PAPH3", "PH3")
    result = result.withColumnRenamed("/BIC/ZPA_PAPH4", "PH4")
    result = result.withColumnRenamed("/BIC/ZPA_PAPH5", "PH5")
    result = result.withColumnRenamed("/BIC/ZPA_PAPH7", "PH7")
    result = result.withColumnRenamed("/BIC/ZPA_PAPH9", "PH9")
    result = result.withColumnRenamed("NAME4", "PH2_desc")
    result = result.withColumnRenamed("NAME5", "PH3_desc")
    result = result.withColumnRenamed("NAME6", "PH4_desc")
    result = result.withColumnRenamed("NAME16", "PH5_desc")
    result = result.withColumnRenamed("NAME14", "PH7_desc")
    result = result.withColumnRenamed("NAME22", "PH9_desc")
    selected_columns = [
      "product_code", "PH2", "PH2_desc", "PH3", "PH3_desc", "PH4", "PH4_desc",
      "PH5", "PH5_desc", "PH7", "PH7_desc", "PH9", "PH9_desc"
    ]
    return result.select(*selected_columns)


# COMMAND ----------

def load_vn_global_product_master() -> PySparkDataFrame:
    """
    Load and process Vietnam product master data.

    This function performs the following operations:
    1. Loads the raw Vietnam product master data using the `load_vn_product_master_raw` function.
    2. Renames and processes columns using the `rename_columns_product_master` function.

    Returns:
    PySparkDataFrame: A PySpark DataFrame containing the processed Vietnam product master data
    with renamed columns.
    """
    raw = load_vn_global_product_master_raw()
    df = rename_columns_product_master(raw)
    df = df.dropDuplicates() # drop for material become unique
    return df

# COMMAND ----------

global_vn_product_master = load_vn_global_product_master()

# COMMAND ----------

display(global_vn_product_master)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore data logic of product master

# COMMAND ----------

# MAGIC %md
# MAGIC - Duplicate of product code due to different /BIC/ZPH_SD1C and NAME3
# MAGIC - Smaller the number -> higher level of grouping. Specifically, PH9 -> more details level. 
# MAGIC - The hierachy of product mapping is like a tree -> not overlapping in the mapping of lower level break down

# COMMAND ----------

global_vn_product_master.count()

# COMMAND ----------

global_vn_product_master.select('product_code').distinct().count()

# COMMAND ----------

display(global_vn_product_master.groupby('product_code').count().orderBy('count', ascending=False)) # checking which product code is duplicated?

# COMMAND ----------

display(global_vn_product_master.filter(F.col("product_code") == "67053403")) # same for the 2 rows

# COMMAND ----------

global_vn_product_master.dropDuplicates().count() # material code can be unique when dropDuplicates()

# COMMAND ----------

raw_product_master = load_vn_global_product_master_raw()

# COMMAND ----------

display(raw_product_master)

# COMMAND ----------

display(raw_product_master.filter(F.col("MATERIAL") == "000000000067053403")) # different due to different in NAME3

# COMMAND ----------

display(raw_product_master.filter(F.col("MATERIAL") == "000000000067053380"))

# COMMAND ----------

global_vn_product_master = global_vn_product_master.dropDuplicates()

# COMMAND ----------

display(global_vn_product_master.groupby("PH2", "PH2_desc").count())

# COMMAND ----------

display(global_vn_product_master.groupby("PH2", "PH2_desc", "PH3_desc").count().orderBy("PH2"))

# COMMAND ----------

display(global_vn_product_master.select("PH2", "PH3").distinct().groupby("PH3", "PH2").count().orderBy("count", ascending=False)) # no PH3 belongs to different PH2

# COMMAND ----------

# MAGIC %md
# MAGIC # Load VN local Product Hierarchy

# COMMAND ----------

def load_vn_local_product_hierarchy_base() -> PySparkDataFrame:
    """
    Load Vietnam local product hierarchy data from a CSV file and perform column transformations.

    This function reads a CSV file containing Vietnam local product hierarchy data and performs
    the following operations:
    
    1. Reads the CSV file from the specified filepath.
    2. Selects specific columns of interest from the loaded DataFrame.
    3. Creates a new 'category_code' column by extracting the first 4 characters from the 'PH7' column.
    4. Renames columns to provide more meaningful names

    Returns:
    PySparkDataFrame: A PySpark DataFrame containing the processed Vietnam local product hierarchy data
    with selected columns and renamed columns.
    """
    filepath = '/mnt/adls/UDL_Gen2/UniversalDataLake/InternalSources/FileFMT/BlobFileShare/SecondarySales/Vietnam/ProdHierarchy/Processed/'
    df = spark.read.format("csv").options(delimiter=",", header="True").load(filepath)
    
    # Select specific columns
    selected_columns = [
        'Material', 'PH9', 'PH9_Desc', 'PH7', 'CD DP Name', 'Portfolio', 'Packsize', 'Packgroup', 'Brand Variant',
        'Subbrand', 'Brand', 'Segment', 'CCBT', 'Small C', 'Big C'
    ]
    df = df.select(*selected_columns)
    
    # Create 'category_code' column
    df = df.withColumn('category_code', F.substring("PH7", 0, 4))
    
    # Rename columns for clarity
    df = df.withColumnRenamed("Material", "product_code")
    df = df.withColumnRenamed("CD DP Name", "cd_dp_name")
    df = df.withColumnRenamed("Portfolio", "portfolio")
    df = df.withColumnRenamed("Packsize", "pack_size")
    df = df.withColumnRenamed("Packgroup", "pack_group")
    df = df.withColumnRenamed("Brand Variant", "brand_variant")
    df = df.withColumnRenamed("Subbrand", "sub_brand")
    df = df.withColumnRenamed("Brand", "brand")
    df = df.withColumnRenamed("Segment", "segment")
    df = df.withColumnRenamed("CCBT", "ccbt")
    df = df.withColumnRenamed("Small C", "small_c")
    df = df.withColumnRenamed("Big C", "big_c")
    df = df.withColumnRenamed("PH9_Desc", "PH9_desc")
    
    return df


# COMMAND ----------

def load_vn_local_product_master(global_product_hierarchy: PySparkDataFrame) -> PySparkDataFrame:
    """
    Retrieve PH7 and PH3 descriptions for the local product hierarchy by joining with the global product hierarchy.

    This function takes two PySpark DataFrames as input and performs the following operations:
    
    1. Selects the 'product_code', 'PH7_desc', and 'PH3_desc' columns from the global product hierarchy.
    2. Joins the local product hierarchy DataFrame with the selected columns from the global product hierarchy
       using a left join operation based on the 'product_code' column.
    
    Args:
    global_product_hierarchy (PySparkDataFrame): A PySpark DataFrame containing global product hierarchy data
        with 'product_code', 'PH7_desc', and 'PH3_desc' columns.

    Returns:
    PySparkDataFrame: A PySpark DataFrame resulting from the join operation, containing the local product hierarchy
    data enriched with 'PH7_desc' and 'PH3_desc' columns from the global product hierarchy.

    Note:
    The 'product_code' column is used as the key for joining the two DataFrames.
    """
    local_product_hierarchy = load_vn_local_product_hierarchy_base()
    global_product_hierarchy = global_product_hierarchy.select("product_code", "PH7_desc", "PH3_desc")
    result = local_product_hierarchy.join(global_product_hierarchy, how="left", on="product_code")
    return result


# COMMAND ----------

local_vn_product_master = load_vn_local_product_master(global_product_hierarchy=global_vn_product_master)

# COMMAND ----------

local_vn_product_master.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Explore data logic

# COMMAND ----------

display(local_vn_product_master)

# COMMAND ----------

display(local_vn_product_master.groupby("big_c", "small_c").count().orderBy("big_c"))

# COMMAND ----------

display(local_vn_product_master.groupBy("category_code").count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Form master data set for featuring

# COMMAND ----------

# MAGIC %md
# MAGIC ## Map history outlet

# COMMAND ----------

def load_daily_sales_and_mapping_outlet_history(from_date: datetime, to_date: datetime) -> PySparkDataFrame:
  daily_sales = spark.read.table("dailysales")
  daily_sales_selected_columns = [
    "transactional_outlet_code", "transactional_site_code", "transactional_distributor_code",
    "product_code", "orig_invoice_date", "net_invoice_val", "invoice_number", 
    "invoicetype", "gross_sales_val"
  ]
  daily_sales = daily_sales.select(*daily_sales_selected_columns)
  daily_sales = daily_sales.filter(F.col("invoicetype") != "ZPR")
  daily_sales = daily_sales.filter((F.col("invoice_date") >= F.lit(from_date)) & (F.col("invoice_date") <= F.lit(to_date)))

  outlet_migration = spark.read.table("outletmigration")
  outlet_migration_selected_columns = [
    "prev_outlet_code", "outlet_code","site_code", "dt_code"
  ]
  outlet_migration = outlet_migration.select(*outlet_migration_selected_columns)
  outlet_migration = outlet_migration.withColumnRenamed("outlet_code", "omi_outlet_code") #omi is outlet migration

  outlet_master = spark.read.table("outletmaster")
  outlet_master_selected_columns = [
    "outlet_code", "group_channel", "channel", "channel_location", "geo_region"
  ]
  outlet_master = outlet_master.select(*outlet_master_selected_columns)
  outlet_master = outlet_master.withColumnRenamed("outlet_code", "oma_outlet_code") #oma is outlet_master
  outlet_master = outlet_master.filter(F.col("group_channel") == "DT")
  outlet_master = outlet_master.filter(F.col("channel") != "1.9. UMP Migration")

  df = daily_sales.join(outlet_migration, on=[daily_sales["transactional_outlet_code"] == outlet_migration["prev_outlet_code"]], how="left")
  df = df.join(outlet_master, on=[df["transactional_outlet_code"] == outlet_master["oma_outlet_code"]], how="left")
  return df

# COMMAND ----------

def rename_column_names(df: PySparkDataFrame) -> PySparkDataFrame:
  """
  The purpose of this function is to rename the column name after mapping old new outlet/distributor/site
  """
  df = df.withColumn("outlet_code", F.when(F.col("prev_outlet_code").isNull(), F.col("transactional_outlet_code")).otherwise(F.col("omi_outlet_code")))
  df = df.withColumnRenamed("transactional_site_code", "old_site_code")
  df = df.withColumn("new_site_code", F.when(F.col("prev_outlet_code").isNull(), F.col("old_site_code")).otherwise(F.col("site_code")))
  df = df.withColumnRenamed("transactional_distributor_code", "old_distributor_code")
  df = df.withColumn("new_distributor_code", F.when(F.col("prev_outlet_code").isNull(), F.col("old_distributor_code")).otherwise(F.col("dt_code")))
  df = df.withColumn("channel", F.when(F.col("group_channel") == "DT", F.col("channel")).otherwise(F.col("group_channel")))
  return df

# COMMAND ----------

def drop_not_use_columns(df:PySparkDataFrame) -> PySparkDataFrame:
  drop_columns = ['invoicetype', 'prev_outlet_code', 'omi_outlet_code']
  df = df.drop(*drop_columns)
  return df

# COMMAND ----------

def load_distributor_master() -> PySparkDataFrame:
  """
  From CD DT side, we only care about the dt_type C10000
  """
  df = spark.read.table("distributormaster")
  df = df.filter(F.col("dt_type") == "C10000")
  return df

# COMMAND ----------

def join_product_mapping_to_master_df(df: PySparkDataFrame, product_mapping:PySparkDataFrame) -> PySparkDataFrame:
  result = df.join(product_mapping, on="product_code", how="left")
  return result

def join_distributor_master_to_master_df(df: PySparkDataFrame, distributor_master:PySparkDataFrame) -> PySparkDataFrame:
  """
  Getting all information of distributors information to master df
  """
  result = df.join(distributor_master, on=df["new_site_code"] == distributor_master["site_code"], how="left")
  return result

# COMMAND ----------

def select_final_columns_and_filter(df: PySparkDataFrame) -> PySparkDataFrame:
  selected_columns = [
    "outlet_code", "orig_invoice_date", "channel", "group_channel", "category_code", "new_distributor_code",
    "new_site_code", "geo_region", "small_c", "PH3_desc", "cd_dp_name", "invoice_number", "product_code", "channel_location",
    "dt_region", "gross_sales_val"
  ]
  result = df.select(*selected_columns)
  result = result.withColumn("orig_invoice_year", F.year(F.col("orig_invoice_date")))
  result = result.withColumn("orig_invoice_month", F.to_date(F.date_format(F.col("orig_invoice_date"), "yyy-MM")))
  result = result.withColumnRenamed("new_site_code", "site_code")
  result = result.withColumnRenamed("new_distributor_code", "distributor_code")
  result = result.withColumnRenamed("PH3_desc", "category_name")

  result = result.filter((F.col("small_c") != "Tea") & (F.col("site_code") != "3001"))
  return result

# COMMAND ----------

def create_master_df(from_date, to_date):
  """
  Combine all the steps to form a master dataframe for feature calculation
  """
  global_product_mapping = load_vn_global_product_master()
  local_product_mapping = load_vn_local_product_master(global_product_hierarchy=global_product_mapping)
  distributor_master = load_distributor_master()
  
  master_df = load_daily_sales_and_mapping_outlet_history(from_date=from_date, to_date=to_date)
  master_df = rename_column_names(df=master_df)
  master_df = drop_not_use_columns(df=master_df)
  master_df = join_product_mapping_to_master_df(df=master_df, product_mapping=local_product_mapping)
  master_df = join_distributor_master_to_master_df(df=master_df, distributor_master=distributor_master)
  master_df = select_final_columns_and_filter(df=master_df)

  return master_df

# COMMAND ----------

from_date = datetime(2021,1,1)
to_date = datetime(2023,11,30)
check = create_master_df(from_date, to_date)

# COMMAND ----------

check.printSchema()

# COMMAND ----------

check.count()

# COMMAND ----------

display(check)

# COMMAND ----------

check_proceeded = rename_column_names(check)

# COMMAND ----------

display(check_proceeded)

# COMMAND ----------

a = drop_not_use_columns(check_proceeded)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore Daily sales

# COMMAND ----------

daily_sales = spark.read.table('dailysales')

# COMMAND ----------

display(daily_sales)

# COMMAND ----------

daily_sales.select("product_code", "invoice_number").distinct().count()

# COMMAND ----------

display(daily_sales.groupby("invoice_number", "product_code").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Questions
# MAGIC - What is the key for daily_sales?
# MAGIC - why invoice_number repeats just different in free_pc_qty
# MAGIC - Why updated to T-2?? not T-1

# COMMAND ----------

display(daily_sales.where("invoice_number = 'IL80015693' and product_code = '75025555'")) 

# COMMAND ----------

daily_sales.count()

# COMMAND ----------

display(daily_sales.select(F.max('orig_invoice_date'), F.min('orig_invoice_date'))) # why updated to T-2???? not T-1?? The result run below is updated when re-execute. Old version run 20231208 date_run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore outlet migration

# COMMAND ----------

# MAGIC %md
# MAGIC - Outlet migration just captures the change outlet. If change in outletcode history -> 1 record
# MAGIC - Just have the latest change of the outletcode

# COMMAND ----------



# COMMAND ----------

outlet_migration = spark.read.table('outletmigration')

# COMMAND ----------

outlet_migration.filter(F.col("prev_outlet_code").isNull()).count()

# COMMAND ----------

display(outlet_migration)

# COMMAND ----------

display(outlet_migration.groupby("prev_outlet_code").count().orderBy("count", ascending=False))

# COMMAND ----------

outlet_migration.count()

# COMMAND ----------

outlet_migration.select("prev_outlet_code").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore outlet master

# COMMAND ----------

# MAGIC %md
# MAGIC - Outlet master to record the change in in outletcode 
# MAGIC - If change 1 time -> then have 1 record
# MAGIC - If not change -> only unique record but prev_outlet_code is null. At the time check (808346/1144599) is null in prev_outlet_code
# MAGIC - Unique by outletcode

# COMMAND ----------

outlet_master = spark.read.table('outletmaster')

# COMMAND ----------

outlet_master.count()

# COMMAND ----------

outlet_master.select("outlet_code").distinct().count()

# COMMAND ----------

outlet_master.filter(F.col("prev_outlet_code").isNull()).count()

# COMMAND ----------

outlet_master.filter(F.col("prev_outlet_code").isNull()).select("outlet_code").distinct().count()

# COMMAND ----------

display(outlet_master.filter(F.col("prev_outlet_code").isNotNull()).orderBy("prev_outlet_code"))

# COMMAND ----------

outlet_master.count()

# COMMAND ----------

display(outlet_master.groupby("prev_outlet_code").count().orderBy("count", ascending=False))

# COMMAND ----------

display(outlet_master.groupby("prev_outlet_code").count().filter(F.col("count") ==1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Questions
# MAGIC Why prev_outlet_coe <> outlet_code even if 1 occurence in outlet master? 

# COMMAND ----------

display(outlet_master.filter(F.col("prev_outlet_code") == "0000765375").orderBy("created_date")) # why outlet_master has 1 record but different in pre_outlet_code and outlet_code????

# COMMAND ----------

# MAGIC %md
# MAGIC ### Questions
# MAGIC - Why mismatch movement between outlet master and outlet migration?

# COMMAND ----------

display(outlet_master.filter(F.col("prev_outlet_code") == "0000348457").orderBy("created_date"))

# COMMAND ----------

outlet_migration.printSchema()

# COMMAND ----------

display(outlet_migration.filter(F.col("prev_outlet_code") == "0000348457"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore distributor master

# COMMAND ----------

# MAGIC %md
# MAGIC - Unique by site code
# MAGIC - 1 distributor has multiple site code
# MAGIC - this table contains the geographic information of distributors

# COMMAND ----------

distributor_master = spark.read.table('distributormaster')

# COMMAND ----------

display(distributor_master)

# COMMAND ----------

distributor_master.count()

# COMMAND ----------

distributor_master.select('dt_code').distinct().count()

# COMMAND ----------

distributor_master.select('site_code').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Calculate Features

# COMMAND ----------

from_date = datetime(2021,1,1)
to_date = datetime(2023,11,30)
master_df = create_master_df(from_date, to_date)

# COMMAND ----------

master_df.cache().count()

# COMMAND ----------

master_df.count()

# COMMAND ----------

display(master_df)

# COMMAND ----------

master_df.select("invoice_number", "product_code").distinct().count()

# COMMAND ----------

master_df.select("invoice_number", "outlet_code", "channel").distinct().count()

# COMMAND ----------

master_df.dropDuplicates().count()

# COMMAND ----------

display(master_df.groupby("product_code", "invoice_number").count().orderBy("count", ascending=False))

# COMMAND ----------

display(master_df.where("product_code = '68867013' and invoice_number = 'FL70028629'").dropDuplicates())

# COMMAND ----------

display(daily_sales.where("invoice_number='FL70028629'"))

# COMMAND ----------

daily_sales.where("invoice_number='FL70028629'").count()

# COMMAND ----------

daily_sales.where("invoice_number='FL70028629'").dropDuplicates().count()

# COMMAND ----------

a_phu_202309 = spark.read.parquet('/mnt/adls/AutoInsight/PropensityScoreInput_lastest/cal_month=2023-09-01')

# COMMAND ----------

a_phu_202309.count()

# COMMAND ----------

display(a_phu_202309)

# COMMAND ----------

a_phu_202309.select("outlet_code", "yearweek", "channel", "small_c", "site_code").distinct().count()

# COMMAND ----------

display(a_phu_202309.groupby("outlet_code", "yearweek", "channel", "small_c", "site_code").count().orderBy("count", ascending=False))

# COMMAND ----------

display(a_phu_202309.where("outlet_code = '0001151206' and yearweek = '202335' and channel= '1.2 MOM & POP' and small_c = 'Oral'"))

# COMMAND ----------

a_phu_202309.dropDuplicates().count()

# COMMAND ----------

