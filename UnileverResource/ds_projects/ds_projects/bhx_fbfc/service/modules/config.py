# Databricks notebook source
import logging
import warnings

logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").disabled = True
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# COMMAND ----------

import multiprocessing

NUM_CPUS = multiprocessing.cpu_count()

# COMMAND ----------

'''
Variables that assign paths to data storage

SALES_OUT_PATH: Path to daily secondary sales report by Bach Hoa Xanh for each store/product
PRODUCT_HIERARCHY_PATH: Path to product hierarchy by Unilever. CURRENTLY NOT A DIRECT ACCESS AS UDL ACCESS DENIED -> OFFLINE VERSION
PRODUCT_MASTER_PATH: Path to product mapping from BHX codes to Unilever codes
'''

SALES_OUT_PATH = '/mnt/adls/MDL_Prod/Bronze/BHX/SaleOutReport/VN/Processed/'
PRODUCT_HIERARCHY_PATH = '/mnt/adls/BHX_FC/FBFC/DATA/PRODUCT_MASTER.csv'
PRODUCT_MASTER_PATH = '/mnt/adls/MDL_Prod/Silver/ModernTrade/Dimension/dim_customer_product_mapping'

# COMMAND ----------

