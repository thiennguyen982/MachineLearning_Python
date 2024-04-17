# Databricks notebook source
# %pip install pandas pyarrow --upgrade
# %pip install sktime
# %pip install ray

# COMMAND ----------

# DBTITLE 1,Get Parameters
dbutils.widgets.text("BANNER_REMOVE_REPEATED", "")
dbutils.widgets.text("BANNER_PROMOTION", "")
dbutils.widgets.text("BANNER_NAME", "")
dbutils.widgets.text("USER_TRIGGER", "")

BANNER_REMOVE_REPEATED = dbutils.widgets.get("BANNER_REMOVE_REPEATED")
BANNER_PROMOTION = dbutils.widgets.get("BANNER_PROMOTION")
BANNER_NAME = dbutils.widgets.get("BANNER_NAME")
USER_TRIGGER = dbutils.widgets.get("USER_TRIGGER")

# BANNER_REMOVE_REPEATED = "2022_AEON REMOVE REPEATED FILE.parquet.gzip_Lai-Trung-Minh Duc.PDF"
# BANNER_PROMOTION = "2022_PROMOTION TRANSFORMATION_UPDATEDAEON.par_Lai-Trung-Minh Duc.PDF"
# BANNER_NAME = "AEON"
# USER_TRIGGER = "Lai-Trung-Minh.Duc@unilever.com"

for check_item in [BANNER_REMOVE_REPEATED, BANNER_PROMOTION, BANNER_NAME, USER_TRIGGER]:
  if check_item == "" or check_item == None:
    raise Exception("STOP PROGRAM - NO DATA TO RUN")
    
print (f"BANNER_REMOVE_REPEATED: {BANNER_REMOVE_REPEATED}")
print (f"BANNER_PROMOTION: {BANNER_PROMOTION}")
print (f"BANNER_NAME: {BANNER_NAME}")
print (f"USER_TRIGGER: {USER_TRIGGER}")

# COMMAND ----------

# DBTITLE 1,Define  INPUT PATH

# Remove repeated (SAlE) path 
PATH_RAW_SALE = f"/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/"    

# PROMOTION INPUT FROM BIZ (FC PROMOTION) path 
PATH_PROMOTION = f"/dbfs/mnt/adls/MT_POST_MODEL/DATA_PROMOTION/UPDATE"   

# HISTORY PROMOTION - STORE IN ADLS
PATH_PROMOTION_HISTORY = f"/dbfs/mnt/adls/MT_POST_MODEL/DATA_PROMOTION/HISTORY/"

######## MASTER DATA PATH ######

PATH_RAW_CALENDAR = (
    f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/CALENDAR_MASTER.xlsx"
)
PATH_PRICE_CHANGE = (
    f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/PRICE_CHANGE_MASTER.xlsx"
)
FACTOR = f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/Machine learning Factors.xlsx"

    
PATH_PRODUCT_MASTER = f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/PRODUCT MASTER.csv"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/MT_POST_MODEL/DATA_RAW_CONVERT/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/MT_POST_MODEL/DATA_PROMOTION/UPDATE/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/MT_POST_MODEL/DATA_PROMOTION/HISTORY/

# COMMAND ----------

# DBTITLE 1,Library Imports
import warnings; warnings.filterwarnings('ignore')


import pandas as pd
import numpy as np
import re
import calendar
import os
import getpass

# from config import model,price_change,raw_promotion,master,user,products_master,price_change,factor_master,calendar_master,promotion_master,clean_promotion,source_promotion,raw,transform_update,raw_run
# from common import list_banner,convert_format,date_time_convert,convert_per,filter_banner,filter_banner_clean,convert_percent
pd.set_option("display.max_columns", None)

# from config import ls_group,ls_agg_normal,ls_agg,transform_mapping
import sys; sys.stdout.fileno = lambda: False
import ray
ray.init(include_dashboard=False, ignore_reinit_error=True)

from sktime import distances
from scipy.cluster.hierarchy import single, complete, average, ward, dendrogram
from scipy.cluster.hierarchy import fcluster


# COMMAND ----------

# DBTITLE 1,STANDARDIZE DATA
def convert_format(df):
    df.columns = map(lambda x: str(x).upper(),df.columns)
    df=df.apply(lambda x: x.astype(str).str.lower())
    return df

# COMMAND ----------

# DBTITLE 1,CONVERT TO DAILY LEVEL AND MAP SALE-PROMOTION- MASTER TOGETHER
def convert_data_raw(df_raw, 
                     df_promotion, 
                     df_calendar, 
                     df_price_change):
    def convert_daily_sale(df_raw):
        from datetime import date, datetime
        #df_raw['ACTUAL SALE (CS)'] = df_raw['BILLING (CS)']
        year = datetime.now().year
        end_year = date(year, 12, 31)
        range_time = pd.date_range("2019-01-01", end_year)
        range_time_str = [str(i).split(" ")[0] for i in range_time]
        df_master = df_raw.loc[:, ["BANNER", "ULV CODE", "DP NAME"]]
        df_master = df_master.drop_duplicates()
        df_master["BILLING DATE_NO_SALE"] = [
            list(range_time_str) for i in df_master.index
        ]
        df_master = df_master.explode("BILLING DATE_NO_SALE")
        df_master["KEY"] = (
            df_master["ULV CODE"] + "-" + df_master["BILLING DATE_NO_SALE"]
        )
        df_raw = df_raw.loc[:, ["BILLING DATE", "EST. DEMAND (CS)", "ULV CODE","ACTUAL SALE (CS)"]]
        df_raw["EST. DEMAND (CS)"] = df_raw["EST. DEMAND (CS)"].astype(float)
        df_raw["ACTUAL SALE (CS)"] = df_raw["ACTUAL SALE (CS)"].astype(float)
        
        df_raw = (
            df_raw.groupby(["BILLING DATE", "ULV CODE"]).agg({"EST. DEMAND (CS)":sum,
                                                                "ACTUAL SALE (CS)":sum}).reset_index()
        )
        df_raw["KEY"] = df_raw["ULV CODE"] + "-" + df_raw["BILLING DATE"]
        df_raw = df_raw.merge(df_master, on="KEY", how="right")
        df_raw["BILLING DATE"] = df_raw["BILLING DATE_NO_SALE"]
        df_raw["ULV CODE"] = df_raw["ULV CODE_y"]
        df_raw = df_raw.loc[
            :,
            [
                "BANNER",
                "ULV CODE",
                "DP NAME",
                "BILLING DATE",
                "EST. DEMAND (CS)",
                "KEY", "ACTUAL SALE (CS)"
            ],
        ]
        df_raw = df_raw.fillna(0)
        return df_raw

    def convert_promotion_daily(df_promotion):
        df_promotion['BILLING DATE'] = 0 
        index_list = df_promotion.index
        ######## ADDING LOGIC TO############3
        for i in index_list:
            start = df_promotion['ORDER START DATE'][i]
            end = df_promotion['ORDER END DATE'][i] 
            range_time = pd.date_range(start,end)
            range_time_str = [str(k).split(" ")[0] for k in range_time]
            df_promotion['BILLING DATE'][i] = range_time_str
        df_promotion = df_promotion.explode('BILLING DATE')
        df_promotion[ 'ULV CODE'] = df_promotion[ 'ULV CODE'].astype(str)
        return df_promotion

    def mapping_promotion(df_daily_sale, 
                          df_promotion):
        df_promotion["BILLING DATE"] = df_promotion["BILLING DATE"].astype(str)
        df_promotion["KEY"] = (
            df_promotion["ULV CODE"] + "-" + df_promotion["BILLING DATE"]
        )
        df_daily_sale = df_daily_sale.merge(df_promotion, on="KEY", how="left")
        df_daily_sale["BILLING DATE"] = df_daily_sale["BILLING DATE_x"]
        df_daily_sale["ULV CODE"] = df_daily_sale["ULV CODE_x"]
        df_daily_sale["BANNER"] = df_daily_sale["BANNER_x"]
        df_daily_sale["DP NAME"] = df_daily_sale["DP NAME_x"]
        df_daily_sale.drop(
            df_daily_sale.filter(regex="_y$").columns, axis=1, inplace=True
        )
        df_daily_sale.drop(
            df_daily_sale.filter(regex="_x$").columns, axis=1, inplace=True
        )
        
        return df_daily_sale

    def mapping_calendar(df_daily_sale, 
                         df_calendar):
        df_calendar["BILLING DATE"] = df_calendar["BILLING DATE"].astype(str)
        df_daily_sale = df_daily_sale.merge(df_calendar, on="BILLING DATE", how="left")
        df_daily_sale.drop(
            df_daily_sale.filter(regex="_y$").columns, axis=1, inplace=True
        )
        df_daily_sale.drop(
            df_daily_sale.filter(regex="_x$").columns, axis=1, inplace=True
        )
        return df_daily_sale

    def mapping_price_change(df_daily_sale, 
                             df_price_change):
        df_daily_sale["KEY_PRICE"] = (
            df_daily_sale["BILLING DATE"] + "_" + df_daily_sale["DP NAME"]
        )
        df_daily_sale = df_daily_sale.merge(df_price_change, on="KEY_PRICE", how="left")
        df_daily_sale["BILLING DATE"] = df_daily_sale["BILLING DATE_x"]
        df_daily_sale["DP NAME"] = df_daily_sale["DP NAME_x"]
        df_daily_sale["BANNER"] = df_daily_sale["BANNER_x"]
        df_daily_sale.drop(
            df_daily_sale.filter(regex="_y$").columns, axis=1, inplace=True
        )
        df_daily_sale.drop(
            df_daily_sale.filter(regex="_x$").columns, axis=1, inplace=True
        )
        df_daily_sale['BILLING DATE'] = pd.to_datetime(df_daily_sale['BILLING DATE'])
        return df_daily_sale

    def check_banner(df_daily_sale):  ## Condition about holiday impact for bhx and aeon
        banner = str(df_daily_sale["BANNER"][0]).upper()
        for i in ["DAY_BEFORE_HOLIDAY_CODE", "DAY_AFTER_HOLIDAY_CODE"]:
            df_daily_sale[i] = pd.to_numeric(df_daily_sale[i], errors="coerce")
            df_daily_sale[i] = df_daily_sale[i].fillna(0)
        if banner in ["BHX", "AEON"]:
            df_daily_sale["COVID_IMPACT"] = df_daily_sale["COVID_IMPACT_SOUTH"]
        if banner in ["METRO"]:
            df_daily_sale["DAY_BEFORE_HOLIDAY_CODE"] = df_daily_sale[
                "DAY_BEFORE_HOLIDAY_CODE"
            ].apply(lambda x: float(x) / 2)
            df_daily_sale["DAY_AFTER_HOLIDAY_CODE"] = df_daily_sale[
                "DAY_AFTER_HOLIDAY_CODE"
            ].apply(lambda x: float(x) / 2)
        return df_daily_sale

    def main_convert_daily():
       # Convert to daily sale
        df_daily = convert_daily_sale(df_raw)

        # Convert to promotion daily sale
        df_promotion_daily = convert_promotion_daily(df_promotion)

        # Mapping daily sale and promotion sale
        df_daily = mapping_promotion(df_daily, df_promotion_daily)

        # Mapping with calendar
        df_daily = mapping_calendar(df_daily, df_calendar)

        # Mapping with price change
        df_daily = df_daily[df_daily['DP NAME'] !=0]
        df_daily = mapping_price_change(df_daily, df_price_change)

        #Check banner to adjust holiday impact
        df_daily = check_banner(df_daily)
        df_daily = df_daily.fillna(0)

        # Process result
        df_daily["BILLING DATE"] = pd.to_datetime(df_daily["BILLING DATE"])
        df_daily = df_daily.sort_values(by="BILLING DATE", ascending=True)
        df_daily["EST. DEMAND (CS)"] = df_daily["EST. DEMAND (CS)"].astype(float)
        df_daily["YEARWEEK"] = df_daily["BILLING DATE"].dt.strftime('%Y%V')
        df_daily["YEARWEEK"] = df_daily["YEARWEEK"].astype(int)
        df_daily = df_daily.drop(columns = ['KEY'],axis = 1)
        return df_daily

    return main_convert_daily()


# COMMAND ----------

# DBTITLE 1,Adjust feature for BHX

def create_BHX_feature(df_daily_sale,
                       df_nso):
    def nso_bhx(df_daily_sale, df_nso):
        banner = str(df_daily_sale["BANNER"][0])
        df_nso = df_nso.loc[:, ["NSO", "CLOSE", "TOTAL NUMBER STORE", "YEARMONTH"]]
        if banner == "bach hoa xanh":
            df_daily_sale["YEARMONTH"] = df_daily_sale["BILLING DATE"].dt.strftime(
                "%m%Y"
            )
            df_daily_sale["YEARMONTH"] = df_daily_sale["YEARMONTH"].apply(
                lambda x: int(x)
            )
            df_daily_sale = df_daily_sale.merge(df_nso, on="YEARMONTH", how="left")
        else:
            df_daily_sale["NSO"] = 0
            df_daily_sale["CLOSE"] = 0
            df_daily_sale["TOTAL NUMBER STORE"] = 0
        return df_daily_sale

    def main_run():
        df_daily = nso_bhx(df_daily_sale, df_nso)
        return df_daily

    return main_run()


# COMMAND ----------

# DBTITLE 1,CREATE POST LEVEL DATA

  def create_post(df_transform):
    # Process noise text in data
    def text_process(text):
        text = str(text)
        if text == "nan":
            text = "0"
        new = text.replace("-", "0")
        new = new.strip()
        if new == "":
            new = "0"
        return new

    #Encode price change data
    def price_change_encode(text):
        encode = 0
        if "price change" in str(text):
            encode = 1
        return encode

     #Groupby post to create demand by post
    def demand_post(df_transform): 
        ls_agg = [
            "EST. DEMAND (CS)",
            #"EST. DEMAND (CS)_CSE",
            "ACTUAL SALE (CS)",
            "DAY_BEFORE_HOLIDAY_CODE",
            "DAY_AFTER_HOLIDAY_CODE",
            "MONTH_TOTAL",
            "WEEK_TOTAL",
            "DAY_TOTAL",
            "%TTS",
            "% BMI",
            "PRICE CHANGE",
            #"% KA INVESTMENT (ON TOP)",
            "COVID_IMPACT",
            "NSO",
            "CLOSE",
            "TOTAL NUMBER STORE",
        ]
        ls_group = [
            "BANNER",
            "YEAR",
            "CATEGORY",
            "DP NAME",
            "POST NAME",
            "STATUS",
            "CODE TYPE",
            "PROMOTION GROUPING",
            "GIFT TYPE",
            "ON/OFF POST",
            "CLASS",
            "PORTFOLIO (COTC)",
            "PACKSIZE",
            "PACKGROUP",
            "BRAND VARIANT",
            "SUBBRAND",
            "PRODUCTION SEGMENT",
            "SEGMENT",
            "FORMAT",
            "BRAND",
        ]  #'BANNER',
        ls_agg_normal = {
            "ORDER START DATE": "min",
            "ORDER END DATE": "max",
            "EST. DEMAND (CS)": "sum",
            #"EST. DEMAND (CS)_CSE": "mean",
            "ACTUAL SALE (CS)": "sum",
            "DAY_BEFORE_HOLIDAY_CODE": "mean",
            "DAY_AFTER_HOLIDAY_CODE": "mean",
            "MONTH_TOTAL": "mean",
            "WEEK_TOTAL": "mean",
            "DAY_TOTAL": "mean",
            "%TTS": "mean",
            "% BMI": "mean",
            "PRICE CHANGE": "mean",
            #"% KA INVESTMENT (ON TOP)": "mean",
            "COVID_IMPACT": "mean",
            "NSO": "mean",
            "CLOSE": "mean",
        }

        df_transform["PRICE CHANGE"] = df_transform["PRICE CHANGE"].apply(
            lambda x: price_change_encode(x)
        )
        for col in ls_agg:
            df_transform[col] = df_transform[col].fillna(0)
            df_transform[col] = df_transform[col].apply(lambda x: text_process(x))
            df_transform[col] = df_transform[col].fillna(0)
            df_transform[col] = df_transform[col].apply(lambda x: float(x))
        df_post = df_transform.groupby(ls_group).agg(ls_agg_normal).reset_index()
        return df_post
      
    # Create feature promotion impact: price or gift
    def promotion_impact(promo):  #
        promo = str(promo)
        impact = str(0)
        word_list = ["discount", "gift"]
        if "discount" in promo:
            impact = "price"
        if "gift" in promo:
            impact = "gift"
        if len(set(promo.split()).intersection(set(word_list))) > 1:
            impact = "gift+price"
        return impact

    def main_run():
        #Create post
        df_post = demand_post(df_transform)
        df_post = df_post.sort_values(
            by=["DP NAME", "ORDER START DATE"], ascending=[True, True]
        )

        # Create feature Post distance
        df_post["KEY"] = df_post[["POST NAME", "DP NAME", "YEAR"]].astype(str).agg("-".join, axis=1)
        df_post = df_post.reset_index(drop = True)
        df_post["NEXT_ORDER"] = df_post["ORDER START DATE"].shift(-1)
        df_post["NEXT_ORDER"][len(df_post)-1] =  df_post["NEXT_ORDER"][len(df_post)-2]
        df_post["POST_DISTANCE"] = (
            df_post["NEXT_ORDER"] - df_post["ORDER END DATE"]
        )
        df_post['POST_DISTANCE'] = df_post['POST_DISTANCE'].apply(lambda x:x.days) 
        df_post["POST_DISTANCE"][df_post["POST_DISTANCE"] < 0] = 0
        df_post["POST_DISTANCE"] = df_post["POST_DISTANCE"].fillna(0)
        df_post["PROMOTION_IMPACT"] = df_post["PROMOTION GROUPING"].apply(
            lambda x: promotion_impact(x)
        )
        df_post ['ORDER START DATE'] = pd.to_datetime(df_post ['ORDER START DATE'])
        df_post ['ORDER END DATE'] = pd.to_datetime(df_post ['ORDER END DATE']) 
        return df_post

    return main_run()


# COMMAND ----------

# DBTITLE 1,MAPPING FEATURE FOR POST LEVEL


def key_event_mapping(df_post, banner, df_event):
    # PROCESS KEYEVENT DATA
    """
    Category in keyevent is not match with promotion data, so we need to stardardize it
    """
    def cate_replace(text):
        ls_1 = [
            "pc up",
            "deo",
            "oral",
            "skin",
            "fabsol",
            "skincleansing",
            "hair",
            "fabsen",
            "tea",
            "culinary",
            "hnh",
        ]
        ls_2 = [
            "hnh",
            "deo",
            "oral",
            "skincare",
            "fabsol",
            "skincleansing",
            "hair",
            "fabsen",
            "tea",
            "savoury",
            "hnh",
        ]
        if text in ls_1:
            index_point = ls_1.index(text)
            new = ls_2[index_point]
        return new

    #### Preprocessing keyevent file
    def key_event(df_event, banner):  
        df_event.columns = map(lambda x: str(x).upper(), df_event.columns)
        df_event = df_event.apply(lambda x: x.astype(str).str.lower())
        df_event["SKINCLEANSING"] = df_event["SKIN CLEANSING"]
        df_event["HNH"] = df_event["HHC"]
        df_event["FABSEN"] = df_event["FAB SEN"]
        df_event["FABSOL"] = df_event["FAB SOL"]
        df_key_banner = df_event[df_event["BANNER"] == banner.lower()].reset_index()
        ls_cate = [
            "PC UP",
            "DEO",
            "ORAL",
            "SKIN",
            "SKINCLEANSING",
            "HAIR",
            "HNH",
            "FABSEN",
            "FABSOL",
            "TEA",
            "CULINARY",
        ]
        df_factor = pd.DataFrame()
        for col in ls_cate:
            factor_cate = df_key_banner[df_key_banner[col] == "x"]
            factor_cate["KEY_EVENT"] = factor_cate["KEY EVENT NAME"]
            df_ = factor_cate.loc[:, ["YEAR", "BANNER", "POST NAME", "KEY_EVENT"]]
            df_["COL"] = col
            df_factor = pd.concat([df_factor,df_])
            df_factor["COL"] = df_factor["COL"].astype(str).str.lower()
            df_factor["CATEGORY"] = df_factor["COL"].apply(lambda x: cate_replace(x))
            df_factor["YEAR"] = pd.to_numeric(df_factor["YEAR"])
            df_factor["YEAR"] = df_factor["YEAR"].astype(str)
            df_factor["KEY"] = df_factor[
                ["YEAR", "BANNER", "POST NAME", "CATEGORY"]
            ].agg("-".join, axis=1)
            df_factor["KEY_EVENT"] = "keyevent"
        return df_factor

    def key_encode(text):
        text = str(text)
        text = text.replace(" ", "")
        if text == "keyevent":
            new = 1
        else:
            new = 0
        return new

    def special_event(text):
        event = 0
        if "hot price" in text:
            event = "hot price"
        if "mnudl" in text:
            event = "mnudl"
        if "pwp" in text:
            event = "pwp"
        return event
    
    # CReate post lost base on the rule: after Key event: 2 and after special promotion 1
    def post_lost(df_post):  
        df_post["LOST_KEY"] = df_post["KEY_EVENT"].shift(1)
        df_post["LOST_SPECIAL"] = np.where(df_post["SPECIAL_EVENT"] != "0", 1, 0)
        df_post["LOST_SPECIAL"] = df_post["LOST_SPECIAL"].shift(1)
        df_post["POST_LOST"] = df_post["LOST_KEY"] * 2 + df_post["LOST_SPECIAL"]
        df_post = df_post.drop(["LOST_SPECIAL", "LOST_KEY"], axis=1)
        return df_post

    def main_run():
        # global df_post
        df_post["YEAR_KEY"] = df_post["YEAR"].astype(str)
        df_post["KEY"] = df_post[["YEAR_KEY", "BANNER", "POST NAME", "CATEGORY"]].agg(
            "-".join, axis=1
        )
        df_factor = key_event(df_event, banner)
        df_factor = df_factor.loc[:, ["KEY", "KEY_EVENT"]]
        df_post_new = df_post.merge(df_factor, on="KEY", how="left")
        df_post_new["SPECIAL_EVENT"] = df_post_new["PROMOTION GROUPING"].apply(
            lambda x: special_event(x)
        )
        df_post_new = df_post_new.drop_duplicates()
        df_post_new["KEY_EVENT"] = df_post_new["KEY_EVENT"].apply(
            lambda x: key_encode(x)
        )
        df_post_new = post_lost(df_post_new)
        df_post_new["KEY"] = df_post_new[["YEAR_KEY", "BANNER", "POST NAME", "DP NAME"]].agg(
            "-".join, axis=1)
        return df_post_new

    return main_run()


# COMMAND ----------

# DBTITLE 1,CREATE OTHER FEATURE

def create_other_feature(df_post, 
                         df_calendar):
    def mt_working(df_calendar, df_post):  # mapping mt workiing date
        df_post["MT"] = 0
        df_calendar["BILLING DATE"] = pd.to_datetime(df_calendar["BILLING DATE"])
        df_calendar["MTWORKINGDAY"] = df_calendar["MTWORKINGDAY"].apply(
            lambda x: int(x)
        )
        df_post["ORDER START DATE"] = pd.to_datetime(df_post["ORDER START DATE"])
        df_post["ORDER END DATE"] = pd.to_datetime(df_post["ORDER END DATE"])
        df_mt = df_calendar.loc[:, ["BILLING DATE", "MTWORKINGDAY"]]
        for i in df_post.index:
            start = df_post["ORDER START DATE"][i]
            end = df_post["ORDER END DATE"][i]
            select_1 = df_mt[
                (df_mt["BILLING DATE"] <= end) & (df_mt["BILLING DATE"] >= start)
            ]
            df_post["MT"][i] = select_1["MTWORKINGDAY"].sum()
        df_post["MTWORKINGSUM"] = df_post["MT"]
        return df_post

    def cycle_feature(df_post):  # creatig cycle feature
        df_post["DAY_SIN"] = np.sin((df_post.DAY - 1) * (2.0 * np.pi / 31))
        df_post["DAY_COS"] = np.cos((df_post.DAY - 1) * (2.0 * np.pi / 31))
        df_post["MNTH_SIN"] = np.sin((df_post.MONTH - 1) * (2.0 * np.pi / 12))
        df_post["MNTH_COS"] = np.cos((df_post.MONTH - 1) * (2.0 * np.pi / 12))
        df_post["WEEK_SIN"] = np.sin((df_post.WEEK) * (2.0 * np.pi / 52))
        df_post["WEEK_COS"] = np.cos((df_post.WEEK) * (2.0 * np.pi / 52))
        return df_post

    import calendar

    def get_week_of_month(date):
        year = date.year
        month = date.month
        day = date.day
        x = np.array(calendar.monthcalendar(year, month))
        week_of_month = np.where(x == day)[0][0]
        if week_of_month == 0:
            week_of_month = 1
        else:
            week_of_month = week_of_month
        return week_of_month

    def calendar_(df_post):
        df_post["ORDER START DATE"] = pd.to_datetime(
            df_post["ORDER START DATE"], format="%d/%m/%Y"
        )
        df_post["MONTH"] = df_post["ORDER START DATE"].dt.month
        df_post["WEEK"] = df_post['ORDER START DATE'].dt.strftime('%U')
        df_post["WEEK"]  =  df_post["WEEK"].astype(int)
        df_post["DAY"] = df_post["ORDER START DATE"].dt.day
        df_post["WEEKDAY"] = df_post["ORDER START DATE"].dt.weekday
        df_post["WEEKMONTH"] = df_post["ORDER START DATE"].apply(
            lambda x: get_week_of_month(x)
        )
        return df_post

    def main_run():
        df_post_new = mt_working(df_calendar, df_post)
        df_post_new = calendar_(df_post_new)
        df_post_new = cycle_feature(df_post_new)

        return df_post_new

    return main_run()


# COMMAND ----------

# DBTITLE 1,RUN 
@ray.remote
def demand_post_create(banner, 
                       df_raw, 
                       df_promotion, 
                       cate,
                       df_calendar, 
                       df_price_change, df_event
):
    df_raw_category = df_raw[df_raw["CATEGORY"] == cate]
    df_promotion_cate = df_promotion[df_promotion["CATEGORY"] == cate]
    if len(df_promotion_cate ) == 0:
      df_post_new = pd.DataFrame()
      return df_post_new
    else:
      df_daily = convert_data_raw(
          df_raw_category, df_promotion_cate, df_calendar, df_price_change
      )
      df_daily = create_BHX_feature(df_daily, df_nso)
      df_transform = df_daily[df_daily["ORDER START DATE"] != 0]
      
      df_post_int = create_post(df_transform)
      df_post = df_post_int
      df_post_new = key_event_mapping(df_post, banner, df_event)
      df_post_new = create_other_feature(df_post_new, df_calendar)
      return df_post_new

#@ray.remote
#def REMOTE_demand_post_create(banner,
 #                              df_raw,
 #                              df_promotion,
 #                              cate, 
 #                              df_calendar, 
 #                              df_price_change, 
 #                             df_event):
 # return demand_post_create(banner, df_raw, df_promotion, cate, df_calendar, df_price_change, df_event)

# COMMAND ----------


def find_promotion_file(banner, FILE_PROMOTIONS):
    banner = banner.upper()
    for i in FILE_PROMOTIONS:
        if banner not in i:
            continue
        else:
            file_promotion = i
    return file_promotion


# COMMAND ----------

# DBTITLE 1,Main Run with Multicore
#def run_banner_ray(df_calendar, 
#                   df_price_change, 
#                   df_event,
#                   df_raw, 
#                   df_promotion):
#    ls_cate = [
#        "fabsol",
#        "hnh",
#        "skincleansing",
#        "savoury",
#        "oral",
#        "hair",
#        "skincare",
#        "deo",
#        "fabsen",
#   ]
#   banner = df_raw['BANNER'].unique()[0]
#     LIST_TASKS = [
#         REMOTE_demand_post_create.remote(
#             banner, df_raw, df_promotion, cate, df_calendar, df_price_change, df_event
#         )
#         for cate in ls_cate
#     ]
#     LIST_TASKS = ray.get(LIST_TASKS)
#   LIST_TASKS = [demand_post_create(banner, df_raw, df_promotion, cate, df_calendar, df_price_change, df_event) for cate in ls_cate]
#    df_banner = pd.concat(LIST_TASKS)
#    return df_banner

#@ray.remote
#def REMOTE_run_banner_ray(df_calendar, df_price_change, df_event, df_raw, df_promotion):
#  return run_banner_ray(df_calendar, df_price_change, df_event, df_raw, df_promotion)

# COMMAND ----------

# DBTITLE 1,Run: Master Data Import 
import pandas as pd

df_calendar = pd.read_excel(PATH_RAW_CALENDAR, engine="openpyxl")
df_price_change = pd.read_excel(PATH_PRICE_CHANGE, engine="openpyxl")
df_event = pd.read_excel(FACTOR, "Key Event", header=1, engine="openpyxl")
df_nso = pd.read_excel(FACTOR, "NSO_BHX", engine="openpyxl")
# df_raw=df_raw[~df_raw['CATEGORY'].isin(['ic','0'])]
df_price_change.columns = map(lambda x: str(x).upper(), df_price_change.columns)
df_price_change = df_price_change.apply(lambda x: x.astype(str).str.lower())
df_price_change["KEY_PRICE"] = (
    df_price_change["BILLING DATE"] + "_" + df_price_change["DP NAME"]
)
df_price_change = df_price_change[df_price_change['BANNER'] == BANNER_NAME.lower()]  # Filter banner

# Comment out to test price-change file for total dataset.
# df_price_change = df_price_change[
#     df_price_change["BANNER"] == "saigon coop"
# ].reset_index(drop=True)
df_product = pd.read_csv (PATH_PRODUCT_MASTER)


# COMMAND ----------

# DBTITLE 1,READ FILE

def read_file(files,header):
  df = pd.read_excel(files,header=header,engine='openpyxl')
  df = convert_format(df)
  return df

@ray.remote  
def REMOTE_read_file(ls_files,header):
  df_total = pd.DataFrame()
  for files in ls_files:
      df = pd.read_excel(PATH_RAW_SALE + files,header=header,engine='openpyxl')
      df = convert_format(df)
      df_total = pd.concat([df_total,df])
  return df_total 


def read_file_promo(files,header):
  df = pd.read_excel( files,header=header,sheet_name = 'Promotion library', engine='openpyxl')
  df = convert_format(df)
  return df

# COMMAND ----------

# DBTITLE 1,CONCAT ALL SALE DATA
banner = BANNER_NAME
sale_files = os.listdir(PATH_RAW_SALE)
sale_files_banner = [f for f in sale_files if banner.upper() in f]
sale_files_banner_ls = np.array_split(sale_files_banner,48)
LIST_TASKS = [REMOTE_read_file.remote(files_ls, 1) for files_ls in sale_files_banner_ls]
task = ray.get(LIST_TASKS)
df_sale = pd.concat(task)
df_sale['ACTUAL SALE (CS)'] = df_sale['BILLING (CS)']

#df_raw = convert_format(df_product)

# COMMAND ----------

col_sale = ['ULV CODE','BILLING DATE','EST. DEMAND (CS)','ACTUAL SALE (CS)','BANNER','SHIP TO']
df_raw = df_sale[col_sale]

# COMMAND ----------

df_product = convert_format(df_product)
df_raw = df_raw.merge(df_product,on = 'ULV CODE', how = 'left')

# COMMAND ----------

name = banner+ "_SALE.parquet"
df_raw.to_parquet(f"/dbfs/mnt/adls/MT_POST_MODEL/DATA_RAW_CONVERT/"+ name)

# COMMAND ----------

# DBTITLE 1,FILLTER PROMOTION
def fillter_promotion (df_promotion,banner):
  df_promotion = df_promotion[df_promotion['BANNER']==banner.lower()].reset_index(drop = True)        
  df_promotion = df_promotion[df_promotion['CODE TYPE']=='normal']
  df_promotion ['ACTUAL SCHEME ID']  =   df_promotion ['ACTUAL SCHEME ID'].apply(lambda x:'cancel' if 'cancel' in x else x)
  df_promotion= df_promotion[df_promotion['ACTUAL SCHEME ID']!='cancel']
  df_promotion['ORDER START DATE']=pd.to_datetime(df_promotion['ORDER START DATE'])
  df_promotion['ORDER END DATE']=pd.to_datetime(df_promotion['ORDER END DATE'])
  return df_promotion


# COMMAND ----------

# DBTITLE 1,CONCAT PROMOTION DATA
import os
promotion_lib = f"/dbfs/mnt/adls/MT_POST_MODEL/PROMOTION_LIB/"
promotion_files = os.listdir(promotion_lib)
df_promotion = pd.DataFrame()
for files in promotion_files:
  df = read_file_promo(promotion_lib + files,1)
  df_promotion = pd.concat([df_promotion,df])
df_promotion ['EST. DEMAND (CS)_CSE'] = df_promotion ['EST. DEMAND (CS)']
df_promotion ['ACTUAL SALE (CS)_CSE'] = df_promotion ['ACTUAL SALE (CS)']
df_promotion = df_promotion[df_promotion['BANNER'] == banner.lower()]
### New promotion
ls_files =  os.listdir(PATH_PROMOTION)
#file_promo_banner = [f for f in ls_files if banner in ls_files]
df_promo_new  = pd.DataFrame()
for files in ls_files:
  df = read_file_promo(PATH_PROMOTION +"/" +files,1)
  df_promo_new = pd.concat([df_promo_new, df])
df_promo_new ['EST. DEMAND (CS)_CSE'] = df_promo_new ['EST. DEMAND (CS)']
df_promo_new ['ACTUAL SALE (CS)_CSE'] = df_promo_new ['ACTUAL SALE (CS)']

col_select=[ 'BANNER','ACTIVITY ID','POST NAME','ACTIVITY NAME','ULV CODE','ULV DESCRIPTION',  # DP name section
              'CODE TYPE','APT','ACTUAL SCHEME ID',  # Select post section
              'PROMOTION GROUPING','GIFT TYPE','%TTS','% BMI','ON/OFF POST', # promotion section
               'YEAR', 'ORDER START DATE','ORDER END DATE','PRELOAD DATE','STATUS', 'START POST IN STORE','END POST IN STORE',
               'EST. DEMAND (CS)_CSE',
           ]
df_promotion = df_promotion.loc[:,col_select]

df_promo_new = df_promo_new.loc[:,col_select]
os.remove(PATH_PROMOTION +"/" +files)
df_promo_new = df_promo_new[df_promo_new['BANNER'] == banner.lower()]


# COMMAND ----------

# DBTITLE 1,PROMOTION_FINAL

df_promotion = pd.concat([df_promotion,df_promo_new])
df_promotion['ORDER START DATE'] = df_promotion['ORDER START DATE'].str.replace(" 00:00:00","")
df_promotion['ORDER END DATE'] = df_promotion['ORDER END DATE'].str.replace(" 00:00:00","")
df_promotion = fillter_promotion (df_promotion,banner)
df_promotion = df_promotion.merge(df_product,on = 'ULV CODE', how = 'left')
df_promotion   = df_promotion .reset_index(drop = True)


# COMMAND ----------

# DBTITLE 1,REMOVE DUPLICATED

# Remove duplicated post [same mechanic but different timing in 2022]
def group_by_ulv_mechanic(df):
   ls_group=['POST NAME',
             'BANNER',
             'CATEGORY',
             'PORTFOLIO (COTC)', 
             'PACKSIZE',
             'PACKGROUP', 
             'BRAND VARIANT', 
             'SUBBRAND',
              'BRAND', 
              'SEGMENT', 
              'FORMAT',
             'PRODUCTION SEGMENT',
             'CLASS',
             'DP NAME',
             'ULV CODE',
             'PROMOTION GROUPING', 
             'CODE TYPE',
             'STATUS',
             'GIFT TYPE',
              '%TTS', 
              '% BMI',
              'ON/OFF POST',
   ]
   ls_agg={'ORDER START DATE': min,
            'ORDER END DATE':max ,
            'YEAR': 'first',
            'EST. DEMAND (CS)_CSE': 'mean',
            'START POST IN STORE': min,
            'END POST IN STORE': max

                 }
   df_post = df.groupby(ls_group).agg(ls_agg).reset_index()
   return df_post
 
 #Remove duplicated post [same timming but different mechanic in 2022]

def group_by_ulv_timing(df):
   ls_group=['POST NAME',
             'BANNER',
             'CATEGORY',
             'PORTFOLIO (COTC)', 
             'PACKSIZE',
             'PACKGROUP', 
             'BRAND VARIANT', 
             'SUBBRAND',
              'BRAND', 
              'SEGMENT', 
              'FORMAT',
             'PRODUCTION SEGMENT',
             'CLASS',
              'DP NAME',
             'ULV CODE',
             'CODE TYPE',
             'STATUS',
             'ORDER START DATE',
             'ORDER END DATE']
             
   ls_agg={'GIFT TYPE': 'first',
           'PROMOTION GROUPING': 'first',
            '%TTS': 'first', 
            '% BMI':'first',
            'ON/OFF POST': 'first',
            'START POST IN STORE': min,
            'END POST IN STORE': max,
            'EST. DEMAND (CS)_CSE': 'mean',
            'YEAR':'first'

                 }
   df_post = df.groupby(ls_group).agg(ls_agg).reset_index()
   return df_post

# COMMAND ----------

df_promotion['EST. DEMAND (CS)_CSE'] = 0

# COMMAND ----------

# DBTITLE 1,Remove dupplicate
df_promotion_2022 = df_promotion[df_promotion['YEAR'] != "2023"]
df_promotion_2023 = df_promotion[df_promotion['YEAR'] == '2023']
df_promotion_2022 = group_by_ulv_mechanic(df_promotion_2022)
df_promotion_2022 = group_by_ulv_timing(df_promotion_2022)
df_promotion_2023 = group_by_ulv_mechanic(df_promotion_2023)
df_promotion_2023 = group_by_ulv_timing(df_promotion_2023)
df_promotion = pd.concat([df_promotion,df_promotion_2023])
df_promotion   = df_promotion .reset_index(drop = True)

# COMMAND ----------


ls_cate =[
        "fabsol",
        "hnh",
        "skincleansing",
        "savoury",
        "oral",
        "hair",
        "skincare",
        "deo",
        "fabsen",
   ]
LIST_TASKS = [demand_post_create.remote(banner, 
                              df_raw, 
                              df_promotion, 
                              cate,
                              df_calendar, 
                              df_price_change, 
                              df_event) for cate in ls_cate]


# COMMAND ----------

task = ray.get(LIST_TASKS)
df_post = pd.concat(task)

# COMMAND ----------

# DBTITLE 1,OUTPUT
#df_post = run_banner_ray(df_calendar, df_price_change, df_event, df_raw, df_promotion)
df_post = df_post.reset_index(drop = True)
df_post = df_post.drop(df_post[(df_post['STATUS']=='finished') & (df_post['EST. DEMAND (CS)']==0)].index)
stringcols = df_post.select_dtypes(include="object").columns
df_post[stringcols] = df_post[stringcols].fillna("").astype(str)

file_output_path = f"/dbfs/mnt/adls/MT_POST_MODEL/DEMAND_POST_MODEL/{BANNER_NAME}.parquet"

df_post.to_parquet(file_output_path)

# COMMAND ----------

