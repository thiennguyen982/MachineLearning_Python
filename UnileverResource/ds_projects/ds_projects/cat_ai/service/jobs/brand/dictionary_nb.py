# Databricks notebook source
import pandas as pd
# from modules.brand_detector import under_score
from loguru import logger

# COMMAND ----------

def open_brand_dict(brand_path: str):  # For brand
    """Open brand dictionary

    Parameters
    ----------
    brand_path : str
        the path to folder contain

    Returns
    -------
    _type_
        return dict_brand and dict_ls_brand
    """

    import os

    BRAND_PATH = brand_path
    ls_file = os.listdir(BRAND_PATH)
    dict_brand = {}
    dict_ls_brand = {}
    ls_cate = ["oral", "deo", "scl"]
    for cate in ls_cate:
        files = [i for i in ls_file if cate in i.lower()]
        df_brand = pd.read_excel((BRAND_PATH + "/" + files[0]), engine="openpyxl")
        df_brand["BRAND_PREDICTION"] = df_brand["BRAND_PREDICTION"].astype(str)
        df_brand["BRAND_FAMILY"] = df_brand["BRAND_FAMILY"].astype(str)
        ls_brand = df_brand["BRAND_PREDICTION"]
        dict_ = dict(zip(df_brand["BRAND_PREDICTION"], df_brand["BRAND_FAMILY"]))
        dict_brand[cate] = dict_
        dict_ls_brand[cate] = ls_brand
        logger.info(f"Loaded category: {cate} with file: {files}")
    return dict_brand, dict_ls_brand
