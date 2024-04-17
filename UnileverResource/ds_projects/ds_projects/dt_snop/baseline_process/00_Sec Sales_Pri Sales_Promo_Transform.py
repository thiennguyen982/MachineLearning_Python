# Databricks notebook source
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

# MAGIC %md
# MAGIC # SECOND SALES TRANSFORM

# COMMAND ----------

BANNER = ["DT HCME", "DT MEKONG DELTA", "DT North", "DT CENTRAL"]
B2B_SERVICE = ["SS B2B SERVICE", "SS B2B HORECA"]
B2B_ATWORK = [
    "SS B2B AT-WORK",
    "PROSPECT OUTLET_PRO CHANNEL",
    "PROSPECT OUTLET - DT",
    "SS B2B INTERNAL STORE",
]

# COMMAND ----------

# DF_ALL = ps.read_parquet("dbfs:/mnt/adls/LTMDUC/HANA_SECSALES")
DF_ALL = ps.read_parquet("dbfs:/mnt/adls/SAP_HANA_DATASET/RAW_DATA/SEC_SALES_BANNER_WEEKLY_PARQUET") # weekly update on monday morning

DF_ALL = DF_ALL[(DF_ALL["BANNER"].isin(BANNER))]
DF_ALL = DF_ALL.to_pandas()
DF_ALL.shape

# COMMAND ----------

DF_ALL["YEARWEEK"].max()

# COMMAND ----------

df_master_product = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx",
    sheet_name=None,
)

df_master_dpname = df_master_product["DP Name Master"]
df_master_dpname = df_master_dpname[["DP Name", "DP Name Current"]]
df_master_dpname.columns = ["DPNAME", "DPNAME CURRENT"]
df_master_dpname = df_master_dpname.astype(str)
df_master_dpname["DPNAME"] = df_master_dpname["DPNAME"].str.strip().str.upper()
df_master_dpname["DPNAME CURRENT"] = (
    df_master_dpname["DPNAME CURRENT"].str.strip().str.upper()
)

df_master_product = df_master_product["Code Master"]
df_master_product = df_master_product[
    ["Category", "SAP Code", "DP name", "Pcs/CS", "NW per CS (selling-kg)"]
]
df_master_product.columns = ["CATEGORY", "MATERIAL", "DPNAME", "PCS/CS", "KG/CS"]
df_master_product = df_master_product.dropna()
df_master_product["MATERIAL"] = df_master_product["MATERIAL"].astype(int)
df_master_product["KG/PCS"] = df_master_product["KG/CS"] / df_master_product["PCS/CS"]
df_master_product["CATEGORY"] = (
    df_master_product["CATEGORY"].astype(str).str.strip().str.upper()
)
df_master_product["DPNAME"] = (
    df_master_product["DPNAME"].astype(str).str.strip().str.upper()
)
df_master_product["CATEGORY"].loc[
    (df_master_product["DPNAME"].str.contains("BRUSH"))
] = "TBRUSH"

df_master_product = df_master_product.merge(df_master_dpname, on="DPNAME")
df_master_product = df_master_product.drop(columns=["DPNAME"])
df_master_product = df_master_product.rename(columns={"DPNAME CURRENT": "DPNAME"})

print (df_master_product.shape)
df_master_product = df_master_product.drop_duplicates(subset='MATERIAL')
print (df_master_product.shape)

# COMMAND ----------

DF_ALL = DF_ALL.rename(
    columns={
        "MATERIAL": "MATERIAL",
        "PCS": "PCS",
        "BANNER": "REGION",
        "GSV": "GSV",
        "YEARWEEK": "YEARWEEK",
    }
)
DF_ALL["MATERIAL"] = DF_ALL["MATERIAL"].astype(int)
DF_ALL["YEARWEEK"] = DF_ALL["YEARWEEK"].astype(int)
DF_ALL["GSV"] = DF_ALL["GSV"].astype(float)
DF_ALL["PCS"] = DF_ALL["PCS"].astype(float)

DF_ALL = DF_ALL.merge(df_master_product, on="MATERIAL")
DF_ALL["KG"] = DF_ALL["PCS"] * DF_ALL["KG/PCS"]
DF_ALL["TON"] = DF_ALL["KG"] / 1000
DF_ALL["CS"] = DF_ALL["PCS"] / DF_ALL["PCS/CS"]
DF_ALL["KPCS"] = DF_ALL["PCS"] / 1000

# COMMAND ----------

DF_ALL["ACTUALSALE"] = DF_ALL["TON"]
DF_ALL["ACTUALSALE"] = np.where(
    DF_ALL["CATEGORY"].isin(["SKINCARE", "IC", "DEO"]),
    DF_ALL["CS"],
    DF_ALL["ACTUALSALE"],
)
DF_ALL["ACTUALSALE"] = np.where(
    DF_ALL["CATEGORY"].isin(["TBRUSH"]), DF_ALL["KPCS"], DF_ALL["ACTUALSALE"]
)

# COMMAND ----------

DF_ALL["REGION"] = DF_ALL["REGION"].replace(
    ["DT CENTRAL", "DT HCME", "DT MEKONG DELTA", "DT North"],
    ["CENTRAL", "HO CHI MINH - EAST", "MEKONG DELTA", "NORTH"],
)

# COMMAND ----------

DF_ALL = (
    DF_ALL.groupby(["REGION", "CATEGORY", "DPNAME", "YEARWEEK"])[
        ["ACTUALSALE", "KG", "CS", "GSV"]
    ]
    .sum()
    .reset_index()
)

DF_ALL.to_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_SEC_SALES_TOTAL_DT_AGG.parquet",
    index=False,
)

# COMMAND ----------

DF_ALL.head()

# COMMAND ----------

# DF_ALL.to_parquet(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_SEC_SALES_COC4_DETAIL.parquet",
#     index=False,
# )

# COMMAND ----------

# DF_NON_B2B = DF_ALL[
#     ~(DF_ALL["ConsumpOccassClass04 (S.Sales)"].isin(B2B_SERVICE + B2B_ATWORK))
# ]
# DF_B2B = DF_ALL[
#     (DF_ALL["ConsumpOccassClass04 (S.Sales)"].isin(B2B_SERVICE + B2B_ATWORK))
# ]

# DF_NON_B2B = (
#     DF_NON_B2B.groupby(["REGION", "CATEGORY", "DPNAME", "YEARWEEK"])[
#         ["ACTUALSALE", "KG", "CS", "GSV"]
#     ]
#     .sum()
#     .reset_index()
# )
# DF_B2B = (
#     DF_B2B.groupby(["REGION", "CATEGORY", "DPNAME", "YEARWEEK"])[
#         ["ACTUALSALE", "KG", "CS", "GSV"]
#     ]
#     .sum()
#     .reset_index()
# )

# DF_NON_B2B.to_parquet(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_SEC_SALES_NONB2B_AGG.parquet",
#     index=False,
# )
# DF_B2B.to_parquet(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_SEC_SALES_B2B_AGG.parquet",
#     index=False,
# )

# COMMAND ----------



# COMMAND ----------

# DF_NON_B2B = DF_NON_B2B[["REGION", "CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]]
# DF_B2B = DF_NON_B2B[["REGION", "CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]]

# COMMAND ----------

# MAGIC %md
# MAGIC # PRIMARY SALES TRANSFORM

# COMMAND ----------

# MAGIC %md
# MAGIC # PROMOTION ONBOARDING
# MAGIC
# MAGIC ## NOTE
# MAGIC
# MAGIC In the future, the dataframe "df_promo_classifier" will be generated by NLP to predict GREEN/YELLOW for each week based on Mechanic/Events Calendar.

# COMMAND ----------

df_business_promotion = read_excel_folder(
    folder_path="/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-PROMO/",
    sheet_name=None,
)
if "ABNORMAL" not in df_business_promotion.columns:
  df_business_promotion['ABNORMAL'] = 'N'
df_business_promotion = df_business_promotion[
    ["REGION", "CATEGORY", "DPNAME", "YEARWEEK", "BIZ_FINAL_ACTIVITY_TYPE", "ABNORMAL"]
]
df_business_promotion['ABNORMAL'] = df_business_promotion['ABNORMAL'].fillna('N')

# COMMAND ----------

DF_ALL = DF_ALL[["REGION", "CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]]

# COMMAND ----------

###################################################
# After Luong/Thinh finish the Promotion Analysis, 
# the AUTO_FINAL_ACTIVITY_TYPE will be generated by the algorithm.
# Now I leave it BLANK.
###################################################
df_promo_classifier = DF_ALL
df_promo_classifier["AUTO_FINAL_ACTIVITY_TYPE"] = "BLANK"

# COMMAND ----------

df_promo_classifier = df_promo_classifier.merge(
    df_business_promotion, on=["REGION", "CATEGORY", "DPNAME", "YEARWEEK"], how="left"
)

df_promo_classifier["BIZ_FINAL_ACTIVITY_TYPE"] = df_promo_classifier[
    "BIZ_FINAL_ACTIVITY_TYPE"
].fillna(df_promo_classifier["AUTO_FINAL_ACTIVITY_TYPE"])

# COMMAND ----------

df_promo_classifier.to_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_PROMO_CLASSIFICATION.parquet"
)

write_excel_dataframe(
    df_promo_classifier,
    groupby_arr=["CATEGORY"],
    dbfs_directory="/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-PROMO/",
    sheet_name="DATA",
)

# COMMAND ----------

df_promo_classifier

# COMMAND ----------

