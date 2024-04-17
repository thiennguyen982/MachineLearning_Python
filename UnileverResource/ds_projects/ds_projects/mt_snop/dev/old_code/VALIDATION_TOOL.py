# Databricks notebook source
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 13:54:48 2023

@author: Tran-Thi.Giau
"""

import pandas as pd 
#import pandera as pa
import numpy as np
import os
import re
from config import *
from common import *
#from pandera import Column, DataFrameSchema, Int, Check
pd.set_option('mode.use_inf_as_na', True)
import warnings
warnings.filterwarnings("ignore")

#################
MASTER_FILE = {'KEYEVENT':factor_master,
               'PRICE_CHANGE':price_change,
               'PROMOTION_MASTER':promotion_master ,
               'QUATER_MASTER':quater_master, # Cannot interpret 'datetime64[ns, UTC]' as a data type
               'CALENDAR_MASTER':calendar_master,
               'PRODUCT_MASTER' : products_master}
                 
PROMOTION_FILE = user + raw_promotion

VALIDATION_PATH = user +'\\VALIDATION TOOL\\CSC MT Data Validation Contract.xlsx'

########## OPEN VAIDATION###############

validation_contract = pd.read_excel(VALIDATION_PATH , sheet_name=None, skiprows=13)

validation_contract.keys()


##### Common def:

def in_range(f,x):  ### Convert to check data range
    if f['colDataRange']=='N':
        return True
    else:
        max_value = f['colDataRange'].split("|")[1]
        min_value = f['colDataRange'].split("|")[0]
        return (x>=int(min_value)) and (x<=int(max_value))
    
    
##### Selection of validation:
def select_field_validate(sheetname): ## Validate only column in model (select in )
    validation_sheet = validation_contract[sheetname].fillna('').to_dict('records')
    validation_sheet_validate = []
    col = []
    for field in validation_sheet:
        f = field
        if f['colRequired'] == 'Y':
            validation_sheet_validate.append(f)
            col.append(f['colName'])  
    return validation_sheet_validate,col

### Validation process#####

# bool(re.match('^[0-5]?[0-9].(?:[0-9]{2})?[0-9]{2}$','111.2019'))

def build_schema_df(df,f):
    types = f['colDataType']
    col = f['colName']
    formats = f['colDataFormat']
    erro = f['colErrorExplanation']
    nul = f['colNullable']
    ranges = f['colDataRange']
    #### Check null: 
    df[col+'_NULL'] = False
    null_mess = erro.split("_")[1]
    if f['colNullable'] == 'N':
        df[col+'_NULL'] = df[col].isna()
        df[col+'_NULL'] = np.where(df[col+'_NULL'], null_mess,"")
    else:
        df[col] = df[col].fillna(0)
        df[col+'_NULL'] =""
    ### check data type 
    types_mess = erro.split("_")[0]
    df[col+'_TYPE_FORMAT'] = True
    if types == 'datetime':
        df[col+'_TYPE_FORMAT']=pd.to_datetime(df[col], format= formats, errors='coerce').isna()
        df[col+'_TYPE_FORMAT'] = np.where(df[col+'_TYPE_FORMAT'], types_mess, "")
    if types =='number':
        df[col+'_TYPE_FORMAT']=df[col].apply(lambda x: bool(re.match(formats,str(x))))
        df[col+'_TYPE_FORMAT'] = np.where(df[col+'_TYPE_FORMAT'],  "",types_mess)
    if types == 'object':
        df[col+'_TYPE_FORMAT'] = ""
     ### check data range
    ranges_mess = erro.split("_")[2]
    df[col+'_RANGE']=df[col].apply(lambda x:in_range(f,x))
    df[col+'_RANGE'] = np.where(df[col+'_RANGE'],  "",ranges_mess)
    df[col+ '_CHECK'] = df[[col+'_NULL', col+'_TYPE_FORMAT',col+'_RANGE']].agg(', '.join, axis=1)
    df[col+ '_CHECK']  = df[col+ '_CHECK'].str.findall(r'[^\s,](?:[^,]*[^\s,])?').str.join(' ')
    return df

def process_result (col,x):
    if len(x)>0:
        value = col.replace("_CHECK","") + ":" + x
    else: 
        value = x
    return value

def validate_df(df,sheetname):
    int_col =list(df.columns)
    validation_sheet,col = select_field_validate(sheetname)
    for f in validation_sheet:
        print(f['colName'])
        df = build_schema_df(df,f)
    col_check = [col for col in df.columns if "_CHECK" in col]
    for col in col_check:
        df[col] = df[col].astype(str).replace(", , ", "")
        df[col] = df[col].apply(lambda x: process_result (col,x))
    # Filter column error
    col_error = list()
    for col in col_check:
        temp=df.copy()
        temp[col] = np.where(temp[col]=='',np.nan,temp[col])
        res = temp[col].isnull().values.all()
        if res == False:
            col_error.append(col)
    print(f'Error columns: {col_error}')
    df['SUMMARY_CHECK'] = df[col_error].agg(','.join, axis=1)
    df['SUMMARY_CHECK'] = df['SUMMARY_CHECK'].apply(lambda x: "" if len (x) <= len(col_check) else x)
    df['FAIL_CASE'] = df['SUMMARY_CHECK'].apply(lambda x: 'x' if len(x)>0 else "")
    int_col.extend(['FAIL_CASE','SUMMARY_CHECK'] + col_error)
    df = df[int_col]
    pattern = re.compile(r'(,\s){2,}')
    #df['SUMMARY_CHECK'] = df['SUMMARY_CHECK'].str.findall(r'[^\s,](?:[^,]*[^\s,])?').str.join(' ')
    return df


def save_file(df,sheetname):
    clean_folder(user + master_validation)
    path = user + master_validation+ sheetname + '.xlsx'
    if 'x' in list(df['FAIL_CASE'].unique()):
        print("KIỂM TRA :", sheetname)
        df.to_excel(path)
    return df

 
###################################

def run_master ():

    ## Clean folder output
    clean_folder(user + master_validation)
    
    ## Price change
    price_changes_path = user+master+factor_master
    df_price=pd.read_excel(price_changes_path,'Price Change',engine='openpyxl')
    path = user + master_validation+ 'PRICE_CHANGE' + '.xlsx'
    df_price = validate_df(df_price,'PRICE_CHANGE')
    path = user + master_validation+ 'PRICE_CHANGE' + '.xlsx'
    price_fail = len(df_price[df_price['FAIL_CASE'] =='x'])
    if  price_fail>0:
        print("KIỂM TRA :", "PRICE_CHANGE")
        df_price.to_excel(path)

    # Key event
    keyevent_path =user + master + factor_master
    df_keyevent = pd.read_excel(keyevent_path,'Key Event',engine='openpyxl',header = 1)
    df_keyevent = validate_df(df_keyevent,'KEYEVENT')
    path = user + master_validation+ 'KEYEVENT' + '.xlsx'
    event_fail = len(df_keyevent[df_keyevent['FAIL_CASE'] =='x'])
    if event_fail >0:
        print("KIỂM TRA :", "KEYEVENT")
        df_keyevent.to_excel(path)



    
    # Promotion_master
    promotion_master_path =user + master + promotion_master
    df_promotion_master=pd.read_csv (promotion_master_path)
    df_promotion_master = validate_df( df_promotion_master,'PROMOTION_MASTER')
    path = user + master_validation+ 'PROMOTION_MASTER' + '.xlsx'
    promo_fail = len(df_promotion_master [df_promotion_master ['FAIL_CASE'] =='x'])
    if promo_fail >0:
        print("KIỂM TRA :", "PROMOTION_MASTER")
        df_promotion_master.to_excel(path)

  
    
    
    # product_master
    product_master_path =user + master + products_master
    df_product_master=pd.read_csv (product_master_path)
    df_product_master = validate_df( df_product_master,'PRODUCT_MASTER')
    product_fail = len( df_product_master [ df_product_master ['FAIL_CASE'] =='x'])
    path = user + master_validation+ 'PRODUCT_MASTER' + '.xlsx'
    if  product_fail >0:
        print("KIỂM TRA :", "PRODUCT_MASTER")
        df_product_master .to_excel(path)

    
    
    # quater_master
    quater_master_path =user + master + quater_master
    df_quater_master=pd.read_excel (quater_master_path)
    df_quater_master = validate_df( df_quater_master,'QUATER_MASTER')
    quater_fail = len( df_quater_master [ df_quater_master ['FAIL_CASE'] =='x'])
    path = user + master_validation+ 'QUATER_MASTER' + '.xlsx'
    if  quater_fail >0:
        print("KIỂM TRA :", "QUATER_MASTER")
        df_quater_master .to_excel(path)
    
    
    # calendar_master
    calendar_master_path =user + master + calendar_master
    df_calendar_master=pd.read_excel (calendar_master_path)
    df_calendar_master = validate_df( df_calendar_master,'CALENDAR_MASTER')
    path = user + master_validation+ 'CALENDAR_MASTER' + '.xlsx'
    calendar_fail = len( df_calendar_master [ df_calendar_master ['FAIL_CASE'] =='x'])
    if  calendar_fail >0:
        print("KIỂM TRA :", "CALENDAR_MASTER")
        df_calendar_master .to_excel(path)
    
    sumfail = calendar_fail+quater_fail +product_fail+promo_fail+event_fail+price_fail
    return sumfail

def run_promotion(banner,promotion_lib_path_input):
    ls=os.listdir(promotion_lib_path_input)
    for file in ls:
        print(file)
        df_promotion = pd.read_excel(promotion_lib_path_input+file)
        col_remove=[e for e in df_promotion.columns if ('_CHECK' in e) or ('FAIL_CASE' in e) or ('_KEYCHECK' in e)]
        df_promotion=df_promotion.drop(columns=col_remove,axis=1)
        df_promotion=revert_column_name(df_promotion)
        df_promotion['Banner'] = df_promotion['Banner'].str.lower()
        df_banner = df_promotion[df_promotion['Banner'] == banner.lower()]
        # Condition not validate
        cond1 = df_banner['Actual Scheme ID'].str.lower().str.contains('cancel', na=False)
        cond2 = df_banner['Category'].str.lower().isin(['ice cream', 'tea', 'food solutions'])
        df_cancel = df_banner[cond1 | cond2]
        df_vali = df_banner[~cond1 & ~cond2]
        df_vali = validate_df(df_vali,'PROMOTION_LIB')
        df_banner = pd.concat([df_vali, df_cancel],axis=0)
        name = file[:-5]
        if 'x' in df_banner['FAIL_CASE'].unique():
            lst_drop = [f'W{e}' for e in range(1,54)]
            col_drop = [e for e in df_banner.columns if e in lst_drop]
            df_banner = df_banner.drop(columns=col_drop, axis=1)
    return df_banner


def run():
    run_master ()
    banner = input('INPUT THE NAME OF BANNER (SAIGON COOP|BIG_C|AEON|LOTTE|METRO|BACH HOA XANH|EMART)')
    promotion_lib_path_input  = input('PROMOTION INPUT PATH , REPLACE "\" BY"/", ex: C:/Users/abc/Music/MT_WEEKLY/') + '/'
    promotion_lib_path_output  = input('PROMOTION OUTPUT PATH, REPLACE "\" BY"/",  ex: C:/Users/abc/Music/MT_WEEKLY/') + '/'
    clean_folder(promotion_lib_path_output)
    run_promotion(banner,promotion_lib_path_input, promotion_lib_path_output)

    # config
    # promotion_lib_path_input = r'C:\Users\Le-Ngoc-Phuong.Trinh\Unilever\CSE - 5. Database\15. Machine Learning FC\ML_MT_POST\VALIDATION RESULT\PROMOTION\Input' + '/'
    # for banner in banner_lst:
    #    print(banner)
    #    promotion_lib_path_output = r'C:\Users\Le-Ngoc-Phuong.Trinh\Unilever\CSE - 5. Database\15. Machine Learning FC\ML_MT_POST\VALIDATION RESULT\PROMOTION\Output' + '/' + banner + '/'
    #    run_promotion(banner,promotion_lib_path_input, promotion_lib_path_output)
    # print("FINISH VALIDATION")

if __name__== "__main__":
    run()
    