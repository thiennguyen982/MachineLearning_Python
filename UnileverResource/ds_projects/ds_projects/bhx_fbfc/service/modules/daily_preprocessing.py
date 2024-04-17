# Databricks notebook source
import ray
import pyspark.sql.functions as F
import pyspark.pandas as ppd
from tqdm import tqdm

# COMMAND ----------

'''
Activate ray.

PARAMS:
    None

RETURN:
    None
'''
def ray_activate():
  if not ray.is_initialized():
    ray.init()

'''
Reset ray.

PARAMS:
    None

RETURN:
    None
'''
def ray_reset():
  if ray.is_initialized():
    ray.shutdown()
    ray.init()
    return
  
  ray.init()

# COMMAND ----------

ray_activate()

# COMMAND ----------

def read_data(SALES_OUT_PATH, PRODUCT_HIERARCHY_PATH, PRODUCT_MASTER_PATH):
  '''
  Read data and create temporary views for further processing. The paths to those files are initialized in ./CONFIG notebook.

  PARAMS:
      SALES_OUT_PATH: Path to daily secondary sales report by Bach Hoa Xanh for each store/product
      PRODUCT_HIERARCHY_PATH: Path to product hierarchy by Unilever
      PRODUCT_MASTER_PATH: Path to product mapping from BHX codes to Unilever codes

  RETURN:
      None
  '''
  (spark.read.load(SALES_OUT_PATH)
             .where("YYYY = 2023")
             .createOrReplaceTempView('sales_df'))

  (spark.read.format("csv")
             .options(delimiter = ",", header = "True")
             .load(PRODUCT_HIERARCHY_PATH)
             .createOrReplaceTempView('product_master'))

  (spark.read.format('delta')
             .load(PRODUCT_MASTER_PATH)
             .where("Country = 'VN'")
             .createOrReplaceTempView('product_mapping'))

# COMMAND ----------

'''
Pre-processing the data using created temporary views:
- product_code:           Temp view to get the mapping from BHX product codes to Unilever internal product codes
- tmp_data:               Temp view to map product_code to sales data
- product_master_rework:  Temp view to remove Ice Cream products
- sales_data:             Final temp view by removing "Kho" and data after 2023-07-31

PARAMS:
    None

RETURN:
    None
'''
def create_sales_data():
  spark.sql("""
              CREATE OR REPLACE TEMP VIEW product_code AS
              SELECT 
                  customer_sku_code, 
                  sku_code
              FROM (
                    SELECT 
                        customer_sku_code, 
                        sku_code, 
                        ROW_NUMBER() OVER (PARTITION BY customer_sku_code ORDER BY sku_code) AS `num`
                    FROM product_mapping
                    WHERE customer_code = 'CUS-BHX'
                    ORDER BY customer_sku_code
                   )
              WHERE `num` = 1;
            """)
  
  spark.sql("""
              CREATE OR REPLACE TEMP VIEW tmp_data AS
              SELECT 
                  T1.STORE_ID, 
                  T1.STORE_NAME, 
                  T1.PRODUCT_ID, 
                  T1.PRODUCT_NAME, 
                  T1.SALES_QTY, 
                  T1.`DATE`, 
                  T2.sku_code AS SKUID
              FROM sales_df AS T1
              LEFT JOIN product_code AS T2
              ON T1.PRODUCT_ID = T2.customer_sku_code;
            """)
  
  spark.sql("""
              CREATE OR REPLACE TEMP VIEW product_master_rework AS
              SELECT *
              FROM product_master
              WHERE `Small C` NOT IN ('Ice Cream');
            """)

  spark.sql("""
              CREATE OR REPLACE TEMP VIEW sales_data AS
              SELECT * 
              FROM (
                    SELECT 
                        T1.*, 
                        T2.`CD DP Name` AS DP_NAME
                    FROM tmp_data AS T1
                    LEFT JOIN product_master_rework AS T2
                    ON T1.SKUID = T2.Material
                   )
              WHERE (SKUID IS NOT NULL) AND (STORE_NAME NOT LIKE ('% Kho %'));
            """)

# COMMAND ----------

'''
Convert store_ids into a list (if needed).

PARAMS:
    store_ids: a single/list of store_id(s)

RETURN:
    store_ids: a list of store_id(s)
'''
def get_store_ids(store_ids):
  # store_ids = spark.sql("""SELECT DISTINCT STORE_ID FROM sales_data""").toPandas()['STORE_ID'].values.tolist()

  if type(store_ids) == int:
    return [store_ids]

  return store_ids

'''
Conver product_ids into a list (if needed).

PARAMS:
    product_ids: a single/list of product_id(s)

RETURN:
    product_ids: a list of product_id(s)
'''
def get_dp_names(dp_names):
  # dp_names = spark.sql("""SELECT DISTINCT `DP_NAME` FROM sales_data""").toPandas()['DP_NAME'].values.tolist()
  
  if type(dp_names) == str:
    return [dp_names]
  
  return dp_names

# COMMAND ----------

'''
Divide the whole data_list into batches.
- data_list structure: [[(<store_id>, <product_id>), <data>]]
- BATCH_SIZE definition: number of (<store_id>, <product_id>) / number of cpus * 3

PARAMS:
    data_list: list of data groups with the above structure

RETURN:
    batches: list of batches divided from the data_list with size = BATCH_SIZE
'''
def to_batches(data_list):
  BATCH_SIZE = round(len(data_list) / NUM_CPUS / 3)
  batches = []

  if BATCH_SIZE < 1:
    BATCH_SIZE = 1

  for i in range(0, len(data_list), BATCH_SIZE):
    batches.append(data_list[i: i + BATCH_SIZE])
  
  return batches

# COMMAND ----------

'''
Fill in the missing dates for each (<store_id>, <product_id>) by batch with the following strategy:
- Set DATE column to be indexes with daily frequency
- Fill in the null values in SALES_QTY column with 0
- Forward fill the null values in other columns
- Reset index to normal. DATE column is now a feature
- Set the datatype of STORE_ID, PRODUCT_ID, SKUID and SALES_QTY to int
- Re-arrange the positions of columns

PARAMS:
    batch: a batch of data group [(<store_id>, <product_id>), <data>] with size BATCH_SIZE

RETURN:
    result: a batch of data group [(<store_id>, <product_id>), <data>] with no missing dates
'''
def date_explode_by_batch(batch, store_dict):
  result = []

  for group in batch:
    data = group[1]

    data['SALES_QTY'] = data['SALES_QTY'].astype('float').astype('int')

    data = data.groupby(['STORE_ID', 'DP_NAME', 'CATEGORY', 'DATE']).agg({'SALES_QTY': 'sum'})
    data.reset_index(inplace = True)

    store_id = data['STORE_ID'].values.tolist()[0]

    data['STORE_PROVINCE'] = [store_dict[store_id]['PROVINCE']] * len(data)
    data['STORE_DISTRICT'] = [store_dict[store_id]['DISTRICT']] * len(data)

    data = data.set_index('DATE').asfreq('D')

    data['SALES_QTY'] = data['SALES_QTY'].fillna(0)
    data = data.ffill()

    data.reset_index(inplace = True)
    data = data.drop_duplicates(subset = ['DATE'])

    data[['STORE_ID']] = data[['STORE_ID']].astype('int')
    data['SALES_QTY'] = data['SALES_QTY'].astype('float').astype('int')

    data = data[['STORE_PROVINCE', 'STORE_DISTRICT', 'STORE_ID', 'DP_NAME', 'CATEGORY', 'DATE', 'SALES_QTY']]

    data.loc[data['SALES_QTY'] < 0, 'SALES_QTY'] = 0

    result.append([group[0], data])

  return result

'''
Parallel version of date_explode_by_batch function

PARAMS:
    batch: a batch of data group [(<store_id>, <product_id>), <data>] with size BATCH_SIZE

RETURN:
    the result from date_explode_by_batch(batch)
'''
@ray.remote
def REMOTE_date_explode_by_batch(batch, store_dict):
  return date_explode_by_batch(batch, store_dict)

# COMMAND ----------

def get_store_dict():
  store_list = spark.sql("""select distinct STORE_ID, STORE_NAME from sales_data""").toPandas()
  store_list = store_list.drop_duplicates(subset = ['STORE_ID'], keep = 'first').sort_values('STORE_ID').reset_index(drop = True)

  store_list['STORE_CODE'] = store_list['STORE_NAME'].apply(lambda x: x[:11].split('_'))
  store_list['STORE_PROVINCE'] = store_list['STORE_CODE'].apply(lambda x: x[1])
  store_list['STORE_DISTRICT'] = store_list['STORE_CODE'].apply(lambda x: x[2])

  store_dict = {}

  store_ids = store_list['STORE_ID'].values.tolist()
  store_provinces = store_list['STORE_PROVINCE'].values.tolist()
  store_districts = store_list['STORE_DISTRICT'].values.tolist()

  for store_id, store_province, store_district in zip(store_ids, store_provinces, store_districts):
    store_dict[store_id] = {'PROVINCE': store_province, 'DISTRICT': store_district}
  
  return store_dict

# COMMAND ----------

'''
A full function that gets lists of store_ids and product_ids, then return a dict of data groups data_dict
- data_dict structure: {(<store_id>, <product_id>): <data>}
- Process of getting data_dict:
  + Create/Edit list of store_ids/product_ids (if needed)
  + Create a querry with the above lists
  + Filter the data using the querry
  + Divide the data into batches with size BATCH_SIZE
  + Fill in missing dates for each batch
  + Gather all into a dictionary

PARAMS:
    store_ids: a single/list of store_id(s) or 'all' for all stores
    product_ids: a single/list of product_id(s) or 'all' for all products

RETURN:
    data_dict: a dictionary of each (<store_id>, <product_id>) group with the above structure
'''
def to_dict(store_ids = 'all', dp_names = 'all'):

  # Create/Edit list of store_ids/dp_names (if needed) and create a querry with those lists
  if store_ids == dp_names == 'all':
    querry = "SELECT * FROM sales_data"
  elif store_ids == 'all':
    dp_names = get_dp_names(dp_names)

    querry = "SELECT * FROM sales_data WHERE `DP_NAME` IN ({})".format(", ".join(["'" + str(i) + "'" for i in dp_names]))
  elif dp_names == 'all':
    store_ids = get_store_ids(store_ids)

    querry = "SELECT * FROM sales_data WHERE STORE_ID IN ({}}".format(", ".join([str(i) for i in store_ids]))
  else:
    store_ids = get_store_ids(store_ids)
    dp_names = get_dp_names(dp_names)

    querry = "SELECT * FROM sales_data WHERE STORE_ID IN ({}) AND `DP_NAME` IN ({})".format(", ".join([str(i) for i in store_ids]), ", ".join(["'" + str(i) + "'" for i in dp_names]))

  # Filter the data using the querry
  data = ppd.sql(querry)
  data = data.to_pandas().reset_index(drop = True)

  data_list = []
  data_dict = {}

  for store_id, df_store in tqdm(data.groupby('STORE_ID')):
    for dp_names, df_store_product in df_store.groupby('DP_NAME'):
      data_list.append([(store_id, dp_names), df_store_product.reset_index(drop = True)])

  store_dict = get_store_dict()
  
  # Divide the data into batches with size BATCH_SIZE
  data_batches = to_batches(data_list)

  # Fill in missing dates for each batch
  result = ray.get([REMOTE_date_explode_by_batch.remote(batch, store_dict) for batch in tqdm(data_batches)])
  
  data_dict = {}

  # Gather all into a dictionary
  for batch in result:
    for data in batch:
      if len(data[1].index) > 70:
        data_dict[data[0]] = data[1]
  
  return data_dict

# COMMAND ----------

'''
Split the data into training and testing sets with the ratio is 9:1, respectively

PARAMS:
    data: the whole dataset to split

RETURN:
    df_train: training set
    df_test: testing set
'''
def train_test_split(data):
  data = data.reset_index(drop = True)
  train_size = round(len(data.index) * 0.9)

  df_train = data.iloc[:train_size]
  df_test = data.iloc[train_size:]
  return df_train, df_test

'''
Parallel version of train_test_split function

PARAMS:
    data: the whole dataset to split

RETURN:
    the return from train_test_split(data) function
'''
@ray.remote
def REMOTE_train_test_split(data):
  return train_test_split(data)

# COMMAND ----------

'''
Split all groups into training and testing sets

PARAMS:
    data_dict: a dictionary of data groups

RETURN:
    to_return: a dictionary of data groups divided into training and testing sets with structure:
               {(<store_id>, <product_id>): [<df_train>, <df_test>]}
'''
def total_train_test_dict(data_dict):
  keys = data_dict.keys()
  results = ray.get([REMOTE_train_test_split.remote(data_dict[key]) for key in tqdm(keys)])

  to_return = {}

  for key, result in zip(keys, results):
    to_return.update({key: list(result) + get_model(result[0])})
  
  return to_return

# COMMAND ----------

def get_model(df_train):
  avg_sales = df_train['SALES_QTY'].mean()

  if avg_sales < 1:
    return ['rule-based low_performance']
  
  days_on_sales = len(df_train.index)

  if days_on_sales < 91:
    return ['rule-based low_num_of_days']
  
  if days_on_sales >= 300:
    return ['normal models'] + ['yafsu']
  
  return ['normal models']

# COMMAND ----------

def to_report(data_dict, key):
  train_size = len(data_dict[key][0].index)
  test_size = len(data_dict[key][1].index)

  print(f"KEY: {key} - TRAIN_SIZE: {train_size} - TEST_SIZE: {test_size}")

  if train_size < 30:
    print('WARNING: TRAIN SIZE < 30')
  
  if test_size < 7:
    print("WARNING: TEST SIZE < 7")
  
  print()

@ray.remote
def REMOTE_to_report(data_dict, key):
  to_report(data_dict, key)

# COMMAND ----------

'''
Full process of pre-processing

PARAMS:
    store_ids: a single/list of store_id(s) or 'all' for all stores           Default: 'all'
    product_ids: a single/list of product_id(s) or 'all' for all products     Default: 'all'

RETURN:
    a dictionary with all groups are split into training and testing sets
'''
def preprocessing(store_ids = 'all', dp_names = 'all'):
  read_data(SALES_OUT_PATH, PRODUCT_HIERARCHY_PATH, PRODUCT_MASTER_PATH)
  create_sales_data()

  return total_train_test_dict(to_dict(store_ids, dp_names))