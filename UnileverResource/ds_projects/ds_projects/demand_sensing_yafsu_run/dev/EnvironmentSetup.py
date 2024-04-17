# Databricks notebook source
# MAGIC %pip install -U pip
# MAGIC %pip install neuralforecast==1.4.0 mlforecast==0.9.0 statsforecast==1.5.0 
# MAGIC %pip install ray==2.6.3 openpyxl pyxlsb category-encoders
# MAGIC %pip install distfit==1.4.5 scipy==1.8.1 sktime pyts==0.12.0
# MAGIC %pip install --force-reinstall -v "openpyxl==3.1.0"
# MAGIC %pip install skforecast ydata-profiling
# MAGIC %pip install "ray[default]"
# MAGIC %pip install pydantic==2.4
# MAGIC %pip install lightgbm 
# MAGIC %pip install xgboost

# COMMAND ----------

# Import datetime libraries
from datetime import datetime
import time

# Import generic libraries
import sys

sys.stdout.fileno = lambda: False

import warnings

warnings.filterwarnings("ignore")

import os
import glob
import traceback
import re
import shutil

from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, Window, DataFrame, SparkSession
from pyspark.sql.types import *

from typing import Iterable

# Import pandas and pyspark libraries
import pandas as pd
import numpy as np
import pyspark.pandas as ps
import pickle as pkl

# COMMAND ----------

# Databricks Spark Configuration
spark.conf.set("spark.sql.execution.arrow.enabled", True)
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
ps.set_option("compute.ops_on_diff_frames", True)

# COMMAND ----------

NUMBER_OF_PARTITIONS = sc.defaultParallelism * 3

# COMMAND ----------

import logging

logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)

# COMMAND ----------

# Ray Cluster Setup
import ray

# runtime_env = {"pip": ["lightgbm", "skforecast", "xgboost", "mlforecast==0.9.0"]}
# try:
#     ray.init(
#         address="auto",
#         _redis_password="d4t4bricks",
#         runtime_env=runtime_env,
#         ignore_reinit_error=True,
#     )
# except:
#     ray.init(runtime_env=runtime_env, ignore_reinit_error=True)
ray.init(_redis_password="d4t4bricks", ignore_reinit_error=True)
ray.cluster_resources()

# COMMAND ----------

