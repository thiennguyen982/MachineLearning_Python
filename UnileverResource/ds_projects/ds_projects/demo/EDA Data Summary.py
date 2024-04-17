# Databricks notebook source
# import pyspark
# from pyspark.sql import SparkSession
# import pyspark.pandas as ps

# spark = SparkSession.builder.getOrCreate()
import pyspark.pandas as ps


# COMMAND ----------

df = spark.read.csv("/mnt/adls/UDL_Gen2/UniversalDataLake/InternalSources/FileFMT/BlobFileShare/SecondarySales/Vietnam/ProdHierarchy/Processed", header = True)

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.csv("/mnt/adls/UDL_Gen2/UniversalDataLake/InternalSources/LeveredgeOnline/SAPECCISR/SAPHANA/T001W/Processed", 
                    header = True,
                    sep = "|",
                    )

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.csv("/mnt/adls/UDL_Gen2/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/SecondarySales/Vietnam/Processed")

# COMMAND ----------

display(df)

# COMMAND ----------

df.to_pandas_on_spark().head()

# COMMAND ----------

df = spark.read.csv("/mnt/adls/BDL_Gen2/BusinessDataLake/CD/SecSales/Global/Online_Countries/Transactional/OnlineInvoiceSKUSales/OnlineInvoiceSKUSales_hist_parquet")

# COMMAND ----------

