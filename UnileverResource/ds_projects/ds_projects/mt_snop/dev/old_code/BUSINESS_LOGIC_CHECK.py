# Databricks notebook source
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 28 14:25:11 2022

@author: Tran-Thi.Giau
"""

import pandas as pd
import numpy as np
import re
from config import *
from common import *
pd.set_option("display.max_columns", None)
import datetime
import os

###### Read all file need: product master, promotion, 

def open_file_need(): # Open file need for model
    df_product_master = pd.read_csv(user + master + "\\"+products_master)
    df_promotion_master = pd.read_csv(user + master + "\\"+promotion_master)
    df_product_master = convert_format(df_product_master)
    df_promotion_master = convert_format(df_promotion_master)
    return df_product_master, df_promotion_master

def open_promotion_file(banner): # Open promotion file by banne, and filter
    raw_promotion_path = user + raw_promotion
    files =os.listdir(raw_promotion_path)
    df_promotion = pd.DataFrame()
    for file in files:
        df = pd.read_excel(raw_promotion_path + "\\"+ file, header = 1)
        df_promotion = df_promotion.append(df,ignore_index = True)
    df_promotion = convert_format( df_promotion)
    df_promotion = df_promotion[df_promotion['BANNER']==banner.lower()].reset_index(drop = True)        
    df_promotion = df_promotion[df_promotion['CODE TYPE']=='normal']
    df_promotion ['ACTUAL SCHEME ID']  =   df_promotion ['ACTUAL SCHEME ID'].apply(lambda x:'cancel' if 'cancel' in x else x)
    df_promotion= df_promotion[df_promotion['ACTUAL SCHEME ID']!='cancel']
    return df_promotion 

########### CHECK INPUT #############
def check_input(df_promotion): # check status column of promotion lib
    df_promotion['STATUS_CHECK'] = df_promotion['STATUS'].apply(lambda x: 0 if x in ['finished','ongoing','incoming'] else 1)
    return df_promotion

######### Check duration of the post:
    

def check_duration_post_by_line(df_promotion): # Duration Must > 0
    df_promotion ['POST_DURATION_IN_PRI'] = (df_promotion['ORDER END DATE'] - df_promotion['ORDER START DATE']).apply(lambda x:x.days)
    df_promotion ['DURATION_POST_CHECK'] = 0
    df_promotion.loc[df_promotion['POST_DURATION_IN_PRI']<=0,'DURATION_POST_CHECK'] = 1
    df_promotion.loc[df_promotion['POST_DURATION_IN_PRI']>0,'DURATION_POST_CHECK'] = 0
    return df_promotion


def check_duration_post_by_dp(df_promotion): # Duration muster >=1
    df_promotion['YEAR'] = df_promotion['YEAR'].astype(str) 
    df_promotion['KEY_DP'] = df_promotion[['POST NAME', 'DP NAME', 'YEAR']].agg('|'.join, axis=1)
    df_promotion_check = df_promotion[['KEY_DP','ORDER START DATE','ORDER END DATE']]
    df_promotion_group = df_promotion_check.groupby('KEY_DP').agg({'ORDER START DATE':min,
                                                                  'ORDER END DATE':max}).reset_index()
    df_promotion_group['POST_DURATION_DP'] = (df_promotion_group['ORDER END DATE'] - df_promotion_group['ORDER START DATE']).apply(lambda x:x.days)
   
    df_promotion_group = df_promotion_group[['KEY_DP','POST_DURATION_DP']]
    df_promotion = df_promotion.merge(df_promotion_group, on = 'KEY_DP', how = 'left')
    df_promotion['DURATION_POST_DP_CHECK'] = 0
    df_promotion.loc[df_promotion['POST_DURATION_DP']>=7,'DURATION_POST_DP_CHECK'] = 0
    df_promotion.loc[df_promotion['POST_DURATION_DP']<7,'DURATION_POST_DP_CHECK'] = 1
    return df_promotion


####### Check the mapping value between promotion grouping and gift type:
    
def convert_format(df): # Standardize data
    df.columns = map(lambda x: str(x).upper(),df.columns)
    df=df.apply(lambda x: x.astype(str).str.lower())
    return df
    
def convert_df_dict(df): 
    dict_col = {}
    for col in df.columns:
        value = list(df[col].unique())
        dict_col[col] = value
    return dict_col
        
### Check mapping promotion grouping and  gift
def check_mapping_promotion(df_promotion, df_promotion_master): 
    df_promotion_master = df_promotion_master.stack().reset_index()
    df_promotion_master['PROMOTION_MASTER_GROUP'] = df_promotion_master['level_1']
    df_promotion_master['GIFT TYPE'] = df_promotion_master[0]
    df_promotion_master = convert_format(df_promotion_master)
    df_promotion_master = df_promotion_master.loc[:,['PROMOTION_MASTER_GROUP','GIFT TYPE']]
    df_promotion_master ['KEY_CHECK'] = df_promotion_master[['PROMOTION_MASTER_GROUP','GIFT TYPE']].agg('|'.join,axis = 1)
    df_promotion_master = df_promotion_master[df_promotion_master['PROMOTION_MASTER_GROUP']!='PROMOTION GROUPING'].reset_index(drop = True)
    df_promotion_master['PROMOTION_MAPPING_CHECK'] = 0
    df_promotion_master = df_promotion_master[['KEY_CHECK','PROMOTION_MAPPING_CHECK']]
    df_promotion['KEY_CHECK'] = df_promotion[['PROMOTION GROUPING', 'GIFT TYPE']].agg('|'.join, axis =1)
    df_promotion = df_promotion.merge(df_promotion_master, on = 'KEY_CHECK', how = 'left')
    df_promotion ['PROMOTION_MAPPING_CHECK'] =  df_promotion ['PROMOTION_MAPPING_CHECK'].fillna(1)
    df_promotion = df_promotion.drop_duplicates()
    df_promotion=df_promotion.drop(columns=['KEY_CHECK'],axis=1)
    return df_promotion


####### Check TTS and BMI################:


def promotion_type(text):
    text = str(text)
    type_promotion = []
    non_sell = ['food non-selling','food sampling',
                'hc non-selling','hc sampling',
                'pc non-selling','pc sampling']
    sell = ['food selling','food selling (pri)',
            'hc selling','hc selling (pri)',
            'pc selling','pc selling (pri)']
    if 'discount' in text.lower(): 
        type_promotion . append('tts')
    if 'gift'  in text.lower() and 'u-gift' not in text.lower():
        type_promotion . append('bmi')
    if 'u-gift' in text.lower():
        text = text.replace("u-gift","")
        text = text.strip()
        text = text.split("_")
        for i in text:
            if i.strip() in  non_sell:
                type_promotion . append('bmi')
            if i.strip() in  sell:
                type_promotion . append('tts')
    type_promotion = list(set(type_promotion))
    type_promotion = " ".join(type_promotion)
    return type_promotion
        

def check_TTS_BMI(df_promotion): # Checnk range of TTS and BMI
    df_promotion['TTS_CHECK'] = df_promotion['%TTS'].apply(lambda x: 0 if (x==0) or ((x>=0.02) and (x<=0.8)) else 1)
    df_promotion['BMI_CHECK'] = df_promotion['% BMI'].apply(lambda x: 0 if (x==0) or( (x>=0.02) and (x<=0.8)) else 1)
    return df_promotion


def check_TTS_BMI_missing(df_promotion): # Checj the missing value of TTS and BMI
    df_promotion['% BMI'] = pd.to_numeric(df_promotion['% BMI'])
    df_promotion['%TTS'] = pd.to_numeric(df_promotion['%TTS'])
    df_promotion ['TTS_MISSING_CHECK'] = 0
    df_promotion ['BMI_MISSING_CHECK'] = 0
    df_promotion['COMBINE'] = df_promotion['PROMOTION GROUPING'] + "_" + df_promotion['GIFT TYPE']
    df_promotion['COMBINE'] = df_promotion['PROMOTION GROUPING'] + "_" + df_promotion['GIFT TYPE']
    df_promotion['TYPE_PROMOTION'] =df_promotion['COMBINE'].apply(lambda x: promotion_type(x))
    df_promotion.loc[(df_promotion['TYPE_PROMOTION'].str.contains('tts')) & (df_promotion['%TTS']==0),'TTS_MISSING_CHECK'] = 1
    df_promotion.loc[(df_promotion['TYPE_PROMOTION'].str.contains('bmi')) & (df_promotion['% BMI'] ==0),'BMI_MISSING_CHECK'] = 1
    #df_promotion.loc[(df_promotion['TYPE_PROMOTION'].str.contains('u-gift')) & (df_promotion['% BMI'] ==0),'BMI_MISSING'] = 1
    df_promotion ['TTS_BMI'] = df_promotion['TTS_MISSING_CHECK'] + df_promotion['BMI_MISSING_CHECK']
   # df_promotion ['TTS_BMI_MISSING_CHECK'] = 0
    #df_promotion.loc[df_promotion['TTS_BMI']>1,'TTS_BMI_MISSING_CHECK'] = 1
    return df_promotion

######### Check overlap information between one post POST|ULV|YEAR
#### Check overlap duplicated 2 lines (all information is duplicated

def check_dupplicated_line_ulv_post(df_promotion):# Apply for all year
    df_promotion['ULV CODE'] = df_promotion['ULV CODE'].astype(str)
    df_promotion['YEAR'] = df_promotion['YEAR'].astype(str) 
    df_promotion['%TTS'] = df_promotion['%TTS'].astype(str)
    df_promotion['% BMI'] = df_promotion['% BMI'].astype(str)
    df_promotion['POST_NAME_ULV'] = df_promotion[['POST NAME', 'ULV CODE', 'YEAR']].agg('|'.join, axis=1)
    df_promotion_check = df_promotion.loc[:,['POST_NAME_ULV','ON/OFF POST','%TTS','% BMI','GIFT TYPE','PROMOTION GROUPING','STATUS','CATEGORY','ORDER START DATE','ORDER END DATE']]
    df_promotion_check['MECHANICS'] = df_promotion_check[['PROMOTION GROUPING','GIFT TYPE','%TTS','% BMI','ON/OFF POST']].agg('|'.join, axis=1)
    df_promotion_check['TIMESTART'] = df_promotion_check['ORDER START DATE'].astype(str)
    df_promotion_check['TIMEEND'] = df_promotion_check['ORDER END DATE'].astype(str)
    df_promotion_check['TIMING'] = df_promotion_check[['TIMESTART','TIMEEND']].agg('|'.join, axis = 1)
    df_promotion_check = df_promotion_check[['POST_NAME_ULV','MECHANICS','TIMING']]
    df_promotion_check = df_promotion_check.groupby(['POST_NAME_ULV','MECHANICS','TIMING']).size().reset_index()
    df_promotion_check['CHECK_LINE_DUP'] =  df_promotion_check[0]
    df_promotion_check = df_promotion_check[['POST_NAME_ULV','CHECK_LINE_DUP']]
    df_promotion_check = df_promotion_check.drop_duplicates()
    df_promotion = df_promotion.merge(df_promotion_check, on ='POST_NAME_ULV', how = 'left')
    df_promotion['CHECK_LINE_DUP'] = df_promotion['CHECK_LINE_DUP'].apply(lambda x: 0 if x ==1 else 1)
    return df_promotion

#same time  or over lap timing but diff mechanic or same mechanic

def check_same_giff_diff_time(df_promotion):# Cùng mechanic but different timing (overlap and not_)
    df_promotion['YEAR'] = df_promotion['YEAR'].astype(str) 
    df_promotion['%TTS'] = df_promotion['%TTS'].astype(str)
    df_promotion['% BMI'] = df_promotion['% BMI'].astype(str)
    df_promotion['POST_NAME_ULV'] = df_promotion[['POST NAME', 'ULV CODE', 'YEAR']].agg('|'.join, axis=1)
    df_promotion_check = df_promotion.loc[:,['POST_NAME_ULV','ON/OFF POST','%TTS','% BMI','GIFT TYPE','PROMOTION GROUPING','STATUS','CATEGORY','ORDER START DATE','ORDER END DATE']]
    df_promotion_check['MECHANICS'] = df_promotion_check[['PROMOTION GROUPING','GIFT TYPE','%TTS','% BMI','ON/OFF POST']].agg('|'.join, axis=1)
    df_promotion_check['TIMESTART'] = df_promotion_check['ORDER START DATE'].astype(str)
    df_promotion_check['TIMEEND'] = df_promotion_check['ORDER END DATE'].astype(str)
    df_promotion_check['TIMING'] = df_promotion_check[['TIMESTART','TIMEEND']].agg('|'.join, axis = 1)
    df_promotion_check = df_promotion_check[['POST_NAME_ULV','MECHANICS','TIMING']]
    df_promotion_check = df_promotion_check.groupby(['POST_NAME_ULV','MECHANICS']).agg({'TIMING': lambda x: list(x)}).reset_index()
    df_promotion_check['TIMING'] = df_promotion_check['TIMING'].apply(lambda x: len(list(set(x))))
    df_promotion_check['SAME_GIFF_DIFF_TIME'] =  df_promotion_check['TIMING']
    df_promotion_check = df_promotion_check[['POST_NAME_ULV','SAME_GIFF_DIFF_TIME']]
    df_promotion_check = df_promotion_check.drop_duplicates()
    df_promotion = df_promotion.merge(df_promotion_check, on ='POST_NAME_ULV', how = 'left')
    df_promotion['SAME_GIFF_DIFF_TIME'] = df_promotion['SAME_GIFF_DIFF_TIME'].apply(lambda x: 0 if x ==1 else 1)
    df_promotion_2023 = df_promotion[df_promotion['YEAR'].isin(["2023","2022"])]
    df_promotion_2022 = df_promotion[~df_promotion['YEAR'].isin(["2023","2022"])]
    df_promotion_2022['SAME_GIFF_DIFF_TIME'] = 0 
    df_promotion = df_promotion_2022.append(df_promotion_2023,ignore_index = True)  # Giống mechanic nhưng khác timing (cả trùng và không trùng) 
    return df_promotion

def check_same_time_diff_gift(df_promotion):# apply only same timing not overlap timing
    df_promotion['YEAR'] = df_promotion['YEAR'].astype(str) 
    df_promotion['%TTS'] = df_promotion['%TTS'].astype(str)
    df_promotion['% BMI'] = df_promotion['% BMI'].astype(str)
    df_promotion['POST_NAME_ULV'] = df_promotion[['POST NAME', 'ULV CODE', 'YEAR']].agg('|'.join, axis=1)
    df_promotion_check = df_promotion.loc[:,['POST_NAME_ULV','ON/OFF POST','%TTS','% BMI','GIFT TYPE','PROMOTION GROUPING','STATUS','CATEGORY','ORDER START DATE','ORDER END DATE']]
    df_promotion_check['MECHANICS'] = df_promotion_check[['PROMOTION GROUPING','GIFT TYPE','%TTS','% BMI','ON/OFF POST']].agg('|'.join, axis=1)
    df_promotion_check['TIMESTART'] = df_promotion_check['ORDER START DATE'].astype(str)
    df_promotion_check['TIMEEND'] = df_promotion_check['ORDER END DATE'].astype(str)
    df_promotion_check['TIMING'] = df_promotion_check[['TIMESTART','TIMEEND']].agg('|'.join, axis = 1)
    df_promotion_check = df_promotion_check[['POST_NAME_ULV','MECHANICS','TIMING']]
    df_promotion_check = df_promotion_check.groupby(['POST_NAME_ULV','TIMING']).agg({'MECHANICS': lambda x: list(x)}).reset_index()
    df_promotion_check['MECHANIC'] = df_promotion_check['MECHANICS'].apply(lambda x: len(list(set(x))))
    df_promotion_check['SAME_TIME_DIFF_GIFT'] =  df_promotion_check['MECHANIC']
    df_promotion_check = df_promotion_check[['POST_NAME_ULV','SAME_TIME_DIFF_GIFT']]
    df_promotion_check = df_promotion_check.drop_duplicates()
    df_promotion = df_promotion.merge(df_promotion_check, on ='POST_NAME_ULV', how = 'left')
    df_promotion['SAME_TIME_DIFF_GIFT'] = df_promotion['SAME_TIME_DIFF_GIFT'].apply(lambda x: 0 if x ==1 else 1)
    df_promotion_2023 = df_promotion[df_promotion['YEAR'].isin(["2023","2022"])]
    df_promotion_2022 = df_promotion[~df_promotion['YEAR'].isin(["2023","2022"])]
    df_promotion_2022['SAME_TIME_DIFF_GIFT'] = 0 
    df_promotion = df_promotion_2022.append(df_promotion_2023,ignore_index = True)  # Giống mechanic nhưng khác timing (cả trùng và không trùng) 
    return df_promotion


def check_overlap_timing_ulv (df_promotion): ## Same post, but timing is overlap
    df_promotion['ULV CODE'] = df_promotion['ULV CODE'].astype(str)
    df_promotion['YEAR'] = df_promotion['YEAR'].astype(str) 
    df_promotion['POST_NAME_ULV'] = df_promotion[['POST NAME', 'ULV CODE', 'YEAR']].agg('|'.join, axis=1)
    df_promotion_check = df_promotion.loc[:,['POST_NAME_ULV','ORDER START DATE','ORDER END DATE','ULV CODE','GIFT TYPE', '%TTS', '% BMI']]
    df_promotion_check = df_promotion_check.groupby(['POST_NAME_ULV','ULV CODE','GIFT TYPE', '%TTS', '% BMI']).agg({'ORDER START DATE':min,
                                                                    'ORDER END DATE':max}).reset_index()
    df_promotion_check['MECHANIC'] = df_promotion_check[['GIFT TYPE', '%TTS', '% BMI']].agg('|'.join, axis=1)
    df_promotion_check = df_promotion_check.sort_values(['ULV CODE','ORDER START DATE'], ascending= True).reset_index()
    df_promotion_new = pd.DataFrame()
    for ulv in df_promotion_check['POST_NAME_ULV'].unique():
        df_promotion_check_ulv = df_promotion_check[df_promotion_check['POST_NAME_ULV']== ulv].reset_index(drop = True)
        df_promotion_check_ulv['ORDER_START_DATE_NEXT_POST'] = df_promotion_check_ulv['ORDER START DATE'].shift(-1)
        df_promotion_check_ulv .loc[len(df_promotion_check_ulv)-1,['ORDER_START_DATE_NEXT_POST']] = df_promotion_check_ulv .loc[len(df_promotion_check_ulv)-1,'ORDER END DATE']
        df_promotion_check_ulv ['EST_OVERLAP_TIMING_BY_POST'] =  df_promotion_check_ulv['ORDER_START_DATE_NEXT_POST'] - df_promotion_check_ulv['ORDER END DATE'] 
        df_promotion_check_ulv ['EST_OVERLAP_TIMING_BY_POST'] = df_promotion_check_ulv['EST_OVERLAP_TIMING_BY_POST'].apply(lambda x: x.days)
        df_promotion_check_ulv ['OVERLAP_TIMING_BY_POST_ULV_CHECK'] = 0
        df_promotion_check_ulv ['CHECK_GIFT_DIFF'] = 0
        for i in range(len(df_promotion_check_ulv)):
           
            if i+1 >= len(df_promotion_check_ulv):
                break
            else:
                mechanic = df_promotion_check_ulv['MECHANIC'][i]
                mechanic_next = df_promotion_check_ulv['MECHANIC'][i+1]
                    
                if df_promotion_check_ulv ['EST_OVERLAP_TIMING_BY_POST'][i] < 0:
                    
                    if  mechanic == mechanic_next:  ## Bị chèn timing giống mechanic
                        df_promotion_check_ulv ['OVERLAP_TIMING_BY_POST_ULV_CHECK'][i] = 1
                        df_promotion_check_ulv ['OVERLAP_TIMING_BY_POST_ULV_CHECK'][i+1] = 1    
                    else:
                        df_promotion_check_ulv ['CHECK_GIFT_DIFF'][i] = 1
                        df_promotion_check_ulv ['CHECK_GIFT_DIFF'][i+1] = 1   ### Bị chèn timing khác mechanic
            
                    
        df_promotion_new = df_promotion_new.append(df_promotion_check_ulv,ignore_index = True)
    df_promotion_new = df_promotion_new.loc [:,['POST_NAME_ULV','EST_OVERLAP_TIMING_BY_POST','OVERLAP_TIMING_BY_POST_ULV_CHECK','CHECK_GIFT_DIFF']]
    df_promotion = df_promotion.merge(df_promotion_new,on = 'POST_NAME_ULV', how = 'left')
    df_promotion = df_promotion.drop_duplicates(subset=['ACTIVITY ID'])
    df_promotion['OVERLAP_TIMING_BY_POST_ULV_CHECK']
    df_promotion['CHECK_GIFT_DIFF'] =  df_promotion['CHECK_GIFT_DIFF'] + df_promotion['SAME_TIME_DIFF_GIFT']
    df_promotion['OVERLAP_TIMING_BY_POST_ULV_CHECK'] =  df_promotion['OVERLAP_TIMING_BY_POST_ULV_CHECK'] + df_promotion['SAME_GIFF_DIFF_TIME']
    df_promotion['CHECK_GIFT_DIFF'] = df_promotion['CHECK_GIFT_DIFF'].apply(lambda x: 1 if x >=1 else 0)
    df_promotion_2023 = df_promotion[df_promotion['YEAR'].isin(["2023","2022"])]
    df_promotion_2022 = df_promotion[~df_promotion['YEAR'].isin(["2023","2022"])]
    df_promotion_2022['CHECK_GIFT_DIFF'] = 0 
    df_promotion_2022['OVERLAP_TIMING_BY_POST_ULV_CHECK'] = 0 
    df_promotion = df_promotion_2022.append(df_promotion_2023,ignore_index = True)
    return df_promotion


######### Check overlap information between ULV CODE different POST NAME

def check_overlap_timing_post (df_promotion):
    df_promotion['ULV CODE'] = df_promotion['ULV CODE'].astype(str)
    df_promotion['YEAR'] = df_promotion['YEAR'].astype(str) 
    df_promotion['POST_NAME_ULV'] = df_promotion[['POST NAME', 'ULV CODE', 'YEAR']].agg('|'.join, axis=1)
    df_promotion_check = df_promotion.loc[:,['POST_NAME_ULV','ORDER START DATE','ORDER END DATE','ULV CODE']]
    df_promotion_check = df_promotion_check.groupby(['POST_NAME_ULV','ULV CODE']).agg({'ORDER START DATE':min,
                                                                    'ORDER END DATE':max})
    df_promotion_check = df_promotion_check.sort_values(['ULV CODE','ORDER START DATE'], ascending= True).reset_index()
    df_promotion_new = pd.DataFrame()
    for ulv in df_promotion_check['ULV CODE'].unique():
        df_promotion_check_ulv = df_promotion_check[df_promotion_check['ULV CODE']== ulv].reset_index(drop = True)
        df_promotion_check_ulv['ORDER_START_DATE_NEXT_POST'] = df_promotion_check_ulv['ORDER START DATE'].shift(-1)
        df_promotion_check_ulv .loc[len(df_promotion_check_ulv)-1,['ORDER_START_DATE_NEXT_POST']] = df_promotion_check_ulv .loc[len(df_promotion_check_ulv)-1,'ORDER END DATE']
        df_promotion_check_ulv ['EST_OVERLAP_TIMING_BY_POST'] = df_promotion_check_ulv['ORDER_START_DATE_NEXT_POST'] - df_promotion_check_ulv['ORDER END DATE']  
        df_promotion_check_ulv ['EST_OVERLAP_TIMING_BY_POST'] = df_promotion_check_ulv['EST_OVERLAP_TIMING_BY_POST'].apply(lambda x: x.days)
        df_promotion_check_ulv ['OVERLAP_TIMING_BY_POST_CHECK'] = 0
        for i in range(len(df_promotion_check_ulv)):
            if i+1 > len(df_promotion_check_ulv):
                break
            else:
                if df_promotion_check_ulv ['EST_OVERLAP_TIMING_BY_POST'][i] < 0:
                    df_promotion_check_ulv ['OVERLAP_TIMING_BY_POST_CHECK'][i] = 1
                    df_promotion_check_ulv ['OVERLAP_TIMING_BY_POST_CHECK'][i+1] = 1  
        df_promotion_new = df_promotion_new.append(df_promotion_check_ulv,ignore_index = True)
    df_promotion_new = df_promotion_new.loc [:,['POST_NAME_ULV','OVERLAP_TIMING_BY_POST_CHECK']]
    df_promotion = df_promotion.merge(df_promotion_new,on = 'POST_NAME_ULV', how = 'left')
    df_promotion = df_promotion.drop_duplicates(subset=['ACTIVITY ID'])
    return df_promotion

def detect_cancel(text):
    if 'cancel' in str(text).lower():
        new='cancel'
    else:
        new=text
    return new


def convert_format_promo(df):
    df.columns = map(lambda x: str(x).upper(),df.columns)
    df=df.apply(lambda x: x.astype(str).str.lower())
    # # condition not validate
    # cond1 = df['ACTUAL SCHEME ID'].str.lower().str.contains('cancel', na=False)
    # cond2 = df['CODE TYPE'].str.lower()=='normal'
    # cond3 = df['CATEGORY']
    df['ACTUAL SCHEME ID']=df['ACTUAL SCHEME ID'].apply(lambda x:detect_cancel(x))
    df=df[df['ACTUAL SCHEME ID']!='cancel']
    df['CODE TYPE'] = df['CODE TYPE'].str.lower() 
    df=df[df['CODE TYPE']=='normal']
    df = df[df['CATEGORY']!='']
    df['CATEGORY'] = df['CATEGORY'].str.lower()
    df = df[df['CATEGORY']!='ice cream']
    df=df.apply(lambda x:  x.lower() if isinstance(x,str) else x)
    # for col in ['PROMOTION GROUPING', 'GIFT TYPE']:
    #     df[col] = df[col].astype(str).fillna('')
    df['%TTS'] = pd.to_numeric(df['%TTS'], errors='coerce').fillna(0)
    df['% BMI'] = pd.to_numeric(df['% BMI'], errors='coerce').fillna(0)
    df['ORDER START DATE']=pd.to_datetime(df['ORDER START DATE'])
    df['ORDER END DATE']=pd.to_datetime(df['ORDER END DATE'])
    df['ORDER START WEEK']=df['YEARWEEK']=df['ORDER START DATE'].dt.strftime('%U%Y')
    return df

def wording_logic(col,x):
    #check_col = [col for col in df.columns if 'CHECK' in col]
    logic_dict = {'STATUS_CHECK': 'STATUS phải là 1 trong 3 giá trị: finished, ongoing, incoming',
                  'DURATION_POST_CHECK': 'Độ dài của post tính theo từng line ở primary <= 0 ngày',
                  'DURATION_POST_DP_CHECK' : 'Độ dài của post tính trên 1 DP Name ở primary < 7 ngày',
                  #'DURATION_POST_SEC_CHECK': 'Độ dài của post ở secondary <7 ngày',
                  'PROMOTION_MAPPING_CHECK': 'Mechanics không tương ứng với Promotion Grouping đã nhập',
                  'TTS_CHECK':'TTS không nằm trong khoảng [2%; 80%] hoặc bằng 0%',
                  'BMI_CHECK': 'BMI không nằm trong khoảng [2%, 80%] hoặc bằng 0%',
                  'TTS_MISSING_CHECK': 'Thiếu TTS',
                  'BMI_MISSING_CHECK':'Thiếu BMI',
                  "CHECK_LINE_DUP": 'Cùng 1 Post|ULV code|Year, có hơn 1 line với cùng mechanics và cùng timing',
                  "CHECK_GIFT_DIFF": "Cùng 1 Post|ULV code|Year, có hơn 1 line với mechanic khác nhau và overlap timing",
                  "OVERLAP_TIMING_BY_POST_ULV_CHECK":"Cùng 1 Post|ULV code|Year, có hơn 1 line với cùng mechanic nhưng khác timing",
                  'OVERLAP_TIMING_BY_POST_CHECK':'Cùng 1 ULV code có timing giữa 2 post bị overlap nhau'}
    if x ==1: 
        string = col + ":" + logic_dict[col]
    else:
        string = ""
    return string



def summary_logic(df):
    check_col = [col for col in df.columns if 'CHECK' in col]
    df['FAIL_CASE'] = df[check_col].sum(axis = 1)
    df['FAIL_CASE']  = df['FAIL_CASE'].apply(lambda x:'x' if x>0 else '')
    for col in check_col:
        error = df[col].sum()
        if error > 0:
            df[col] = df[col].apply(lambda x: wording_logic(col,x))
        else:
            df=df.drop(columns=col, axis=1)
    # df['CHECK'] = df[check_col].apply(','.join, axis=1)
    return df

def run_code(banner,promotion_lib_path_input):
    df_product_master, df_promotion_master = open_file_need()
    ls=os.listdir(promotion_lib_path_input)
    df_promo_raw = pd.DataFrame()
    for file in ls:
        df_promotion = pd.read_excel(promotion_lib_path_input+file)
        col_remove=[e for e in df_promotion.columns if ('CHECK' in e) or ('FAIL_CASE' in e) or ('_KEYCHECK' in e)
                    or e in ['POST_NAME_ULV', 'KEY_DP']]
        df_promotion=df_promotion.drop(columns=col_remove,axis=1)
        df_promotion=revert_column_name(df_promotion)
        df_promotion['Banner'] = df_promotion['Banner'].str.lower()
        df_banner = df_promotion[df_promotion['Banner'] == banner.lower()]
        df_promo_raw = df_promo_raw.append(df_banner,ignore_index = True)
    
    if len(df_promo_raw) > 0:
        df_promo = convert_format_promo(df_promo_raw) # Standardize & filter data
        cols =  df_promo.columns.values.tolist()
        df_promo =  check_TTS_BMI_missing(df_promo) 
        df_promo = check_input(df_promo)
        df_promo = check_duration_post_by_line(df_promo)
        df_promo = check_duration_post_by_dp(df_promo)
        df_promo = check_mapping_promotion(df_promo, df_promotion_master)
        df_promo = check_TTS_BMI(df_promo)
        df_promo = check_dupplicated_line_ulv_post(df_promo)
        df_promo = check_same_giff_diff_time(df_promo)
        df_promo = check_same_time_diff_gift(df_promo)
        df_promo = check_overlap_timing_ulv (df_promo)
        df_promo = check_overlap_timing_post (df_promo)
        df_promo = summary_logic(df_promo)
        new_cols = cols + ['POST_NAME_ULV','KEY_DP','FAIL_CASE'] + [e for e in df_promo.columns if ('CHECK' in e)]
        df_promo = df_promo[new_cols]
    else:
        df_promo = pd.DataFrame()
        print('CLEAN FORMAT ERROR BEFORE GOING TO NEXT STEP')
    return df_promo
    ####### Report making: 


def main_run():
    banner = input('INPUT THE NAME OF BANNER (SAIGON COOP|BIG_C|AEON|LOTTE|METRO|BACH HOA XANH|EMART)')
    promotion_lib_path_input  = input('PROMOTION INPUT PATH, REPLACE "\" BY"/", ex: C:/Users/abc/Music/MT_WEEKLY/') + '/'
    promotion_lib_path_output  = input('PROMOTION OUTPUT PATH, REPLACE "\" BY"/",  ex: C:/Users/abc/Music/MT_WEEKLY/') + '/'
    df_promo = run_code(banner, promotion_lib_path_input)
    name = banner + "Logic_report.txt"
    name_data = banner +"Data Logic Validaiton.xlsx"
    #with open(    promotion_lib_path_output +name,'w') as data:
    #    data.write(str(dict_report))
        
    df_promo.to_excel(promotion_lib_path_output +name_data)
    
    print("FINSH VALIDATION")
    
if __name__== "__main__":
  main_run()