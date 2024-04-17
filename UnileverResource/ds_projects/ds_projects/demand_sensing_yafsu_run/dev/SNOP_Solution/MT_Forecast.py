# Databricks notebook source
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

# MAGIC %md
# MAGIC # MT SNOP Data Transformation

# COMMAND ----------

DF = pd.read_parquet("/dbfs/mnt/adls/SAP_HANA_DATASET/RAW_DATA/PRI_SALES_BANNER_WEEKLY_PARQUET")
DF = DF.dropna()
DF['MATERIAL'] = DF['MATERIAL'].astype(int)
DF.head(100)

# COMMAND ----------

df_master_product = pd.read_excel("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx", sheet_name = None)

df_master_dpname = df_master_product['DP Name Master']
df_master_dpname = df_master_dpname[['DP Name', 'DP Name Current']]
df_master_dpname.columns = ['DPNAME', 'DPNAME CURRENT']
df_master_dpname = df_master_dpname.astype(str)
df_master_dpname['DPNAME'] = df_master_dpname['DPNAME'].str.strip().str.upper()
df_master_dpname['DPNAME CURRENT'] = df_master_dpname['DPNAME CURRENT'].str.strip().str.upper()

df_master_product = df_master_product['Code Master']
df_master_product = df_master_product[['Category', 'SAP Code', 'DP name', 'Pcs/CS', 'NW per CS (selling-kg)']]
df_master_product.columns = ['CATEGORY', 'MATERIAL', 'DPNAME', 'PCS/CS', 'KG/CS']
df_master_product = df_master_product.dropna()
df_master_product['MATERIAL'] = df_master_product['MATERIAL'].astype(int)
df_master_product['KG/PCS'] = df_master_product['KG/CS']/df_master_product['PCS/CS']
df_master_product['CATEGORY'] = df_master_product['CATEGORY'].astype(str).str.strip().str.upper()
df_master_product['DPNAME'] = df_master_product['DPNAME'].astype(str).str.strip().str.upper()
df_master_product['CATEGORY'].loc[(df_master_product['DPNAME'].str.contains('BRUSH'))] = "TBRUSH"

df_master_product = df_master_product.merge(df_master_dpname, on='DPNAME')
df_master_product = df_master_product.drop(columns = ['DPNAME'])
df_master_product = df_master_product.rename(columns={'DPNAME CURRENT': 'DPNAME'})

# COMMAND ----------

DF = DF.merge(df_master_product, on='MATERIAL', how='inner')
DF = DF.query("PCS > 0.0")

BANNER_LIST = ['AEON', 'METRO', 'WINMART', 'SAIGON COOP', 'BACH HOA XANH',
'PHARMACITY', 'OTHERS', 'MT INDEPENDENT SOUTH', 'MT INDEPENDENT NORTH',
'WINMART PLUS', 'LOTTE', 'ECUSTOMER', 'DT CENTRAL', 'DT HCME',
'DT MEKONG DELTA', 'LAN CHI', 'DT North', 'E-DISTRIBUTOR ', 'EMART',
'BIG_C', 'Giant']

DF = DF[(DF['BANNER'].isin(BANNER_LIST))]

# COMMAND ----------

DF['CS'] = DF['PCS'] * DF['PCS/CS'] 
DF['TON'] = DF['PCS'] / DF['KG/PCS'] / 1000

# COMMAND ----------

DF = DF.groupby(['YEARWEEK', 'BANNER', 'REGION', 'CATEGORY', 'DPNAME'])[['PCS', 'CS', 'TON']].sum().reset_index()

# COMMAND ----------

DF['YEARWEEK'] = DF['YEARWEEK'].astype(int)

# COMMAND ----------

DF['FUTURE'] = "N"
DF['FUTURE'].loc[(DF['YEARWEEK'] >= 202240)] = "Y"

# COMMAND ----------

# DF.to_parquet("/dbfs/mnt/adls/SAP_HANA_DATASET/MT_SNOP_FORECAST.parquet", index=False)

# COMMAND ----------

def write_excel_dataframe(df_all, groupby_arr, file_name, sheet_name, dbfs_directory):
  tasks = [REMOTE_write_excel_file.remote(df_group, file_name + "_" + key, sheet_name, dbfs_directory) for key, df_group in df_all.groupby(groupby_arr)]
  tasks = ray.get(tasks)

write_excel_dataframe(df_all=DF, groupby_arr=['CATEGORY'], file_name="MT_SNOP_FORECAST_CATE", sheet_name="DATA", dbfs_directory="/dbfs/mnt/adls/SAP_HANA_DATASET/YAFSU_MT")

# COMMAND ----------

DF