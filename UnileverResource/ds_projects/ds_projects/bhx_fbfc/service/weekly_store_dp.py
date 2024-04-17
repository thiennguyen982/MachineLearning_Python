# Databricks notebook source
# MAGIC %pip install catboost

# COMMAND ----------

# MAGIC %run ./Modules/FBFC_WEEKLY_PRE_PROCESSING

# COMMAND ----------

import catboost as cb

# COMMAND ----------

# First-step configurations

# Disable warnings
logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").disabled = True
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# Config matplotlib's graphs
plt.rcParams['figure.figsize'] = [40, 20]
plt.rcParams['font.size'] = 20

%matplotlib inline

# Config Pandas's float to .xx
pd.options.display.float_format = "{:,.2f}".format

# Turn on Ray
if not ray.is_initialized():
  ray.init()

# COMMAND ----------

def check(data):
  print(f"Size: {data.count()}")
  print(f"Num IDs: {data.select('STORE_ID', 'DP_NAME').dropDuplicates().count()}")
  print(f"Duplicated Records: {data.select('STORE_ID', 'DP_NAME', 'YEARWEEK').count() - data.select('STORE_ID', 'DP_NAME', 'YEARWEEK').dropDuplicates().count()}")
  print(f"Null State:")
  null_check(data)

  if 'YEARWEEK' in data.columns:
    data.agg(F.min('YEARWEEK'), F.max('YEARWEEK')).display()
  else:
    data.agg(F.min('DATE'), F.max('DATE')).display()

def null_check(data):
  for col in data.columns:
    print(f" - {col}: {data.filter(F.col(col).isNull()).count()}")

# COMMAND ----------

data = (spark.sql('select * from sales_data')
             .select('STORE_ID', 'DP_NAME', 'DATE', 'SALES_QTY')
             .withColumn('SALES_QTY', F.when(F.col('SALES_QTY') < 0, 0).otherwise(F.col('SALES_QTY')))
             .withColumn('SALES_QTY', F.col('SALES_QTY').cast('int'))
             .orderBy('STORE_ID', 'DP_NAME', 'DATE'))

date_exploded = (data.groupBy('STORE_ID', 'DP_NAME')
                     .agg(
                          F.min('DATE').alias('MIN_DATE'),
                          F.max('DATE').alias('MAX_DATE')
                         )
                     .withColumn('EXPLODED_DATE', F.explode(F.expr('sequence(MIN_DATE, MAX_DATE, interval 1 day)'))))

tmp = (data.withColumnRenamed('STORE_ID', 'STORE_ID_x')
           .withColumnRenamed('DP_NAME', 'DP_NAME_x'))

conds = [date_exploded.STORE_ID == tmp.STORE_ID_x, date_exploded.DP_NAME == tmp.DP_NAME_x, date_exploded.EXPLODED_DATE == tmp.DATE]

date_exploded = (date_exploded.join(tmp, conds, 'left')
                              .drop('STORE_ID_x', 'DP_NAME_x', 'DATE', 'MIN_DATE', 'MAX_DATE')
                              .withColumnRenamed('EXPLODED_DATE', "DATE")
                              .na.fill(0)
                              .orderBy('STORE_ID', 'DP_NAME', 'DATE'))

# COMMAND ----------

# date_exploded.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231108_TMP_DE.parquet')
date_exploded = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231108_TMP_DE.parquet')

# COMMAND ----------

data = date_exploded.withColumn('YEARWEEK', F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7))

date_mapping = (data.select('DATE', 'YEARWEEK')
                    .dropDuplicates()
                    .withColumn('DAY_OF_WEEK', F.dayofweek('DATE'))
                    .withColumn('UNIX_TIMESTAMP', F.unix_timestamp('DATE'))
                    .filter(F.col('DAY_OF_WEEK') == 5)
                    .drop('DATE', 'DAY_OF_WEEK')
                    .withColumnRenamed('YEARWEEK', 'YEARWEEK_x')
                    .orderBy('UNIX_TIMESTAMP'))

data = (data.drop('DATE')
            .join(date_mapping, data.YEARWEEK == date_mapping.YEARWEEK_x, 'left')
            .drop('YEARWEEK_x')
            .groupBy('STORE_ID', 'DP_NAME', 'YEARWEEK', 'UNIX_TIMESTAMP')
            .agg(F.sum('SALES_QTY').alias('SALES_QTY'))
            .na.drop()
            .orderBy('STORE_ID', 'DP_NAME', 'YEARWEEK'))

# COMMAND ----------

# data.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231108_TMP_DATA_EXPLODED.parquet')
data = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231108_TMP_DATA_EXPLODED.parquet')

# COMMAND ----------

def custom_median(*args):
  return float(np.median(list(args)))

udf_median = F.udf(custom_median, types.DoubleType())

# COMMAND ----------

lag_rolling = [
                (4, 4), (8, 4), (12, 4),
                (4, 12)
              ]

week = lambda x: x * 604800

for lr in lag_rolling:
  lag = lr[0]
  rolling = lr[1]

  w = Window().partitionBy('STORE_ID', 'DP_NAME').orderBy('UNIX_TIMESTAMP').rangeBetween(-week(lag + rolling), -week(lag + 1))

  data = (data.withColumn('R_SUM_' + str(lag) + '_' + str(rolling), F.sum('SALES_QTY').over(w))
              .withColumn('R_MEAN_' + str(lag) + '_' + str(rolling), F.avg('SALES_QTY').over(w))
              .withColumn('R_MEDIAN_' + str(lag) + '_' + str(rolling), F.expr('percentile(SALES_QTY, 0.5)').over(w))
              .withColumn('R_MAX_' + str(lag) + '_' + str(rolling), F.max('SALES_QTY').over(w)))

w = Window().partitionBy('STORE_ID', 'DP_NAME').orderBy('YEARWEEK')

lags = [4, 5, 6, 7, 8, 12, 16]

for i in lags:
  data = data.withColumn('LAG_' + str(i), F.lag('SALES_QTY', i).over(w))

data = data.na.drop()

# COMMAND ----------

# data.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231108_TMP_DATA_LAG_ROLLING.parquet')
data = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231108_TMP_DATA_LAG_ROLLING.parquet')

# COMMAND ----------

packsize_mapping = spark.createDataFrame(pd.read_csv('/dbfs/mnt/adls/BHX_FC/FBFC/DATA/PACKSIZE_MAPPING.csv'))

data = (data.drop('PACKSIZE')
            .join(packsize_mapping, data.DP_NAME == packsize_mapping.DP_NAME, 'left')
            .drop(packsize_mapping.DP_NAME)
            .withColumnRenamed('PACKSIZE_GR', 'PACKSIZE'))

# COMMAND ----------

w = Window().partitionBy('STORE_ID', 'DP_NAME').orderBy('YEARWEEK')

active_mapping = (spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DE.parquet')
                       .withColumn('YEARWEEK', F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7))
                       .groupBy('STORE_ID', 'DP_NAME', 'YEARWEEK')
                       .agg(
                              F.count('SALES_QTY').alias('TOTAL_DAYS'),
                              F.count(F.when(F.col('SALES_QTY') > 0, True)).alias('ACTIVE_DAYS_CURRENT')
                           )
                       .withColumn('ACTIVE_DAYS_LAG_4', F.lag('ACTIVE_DAYS_CURRENT', 4).over(w))
                       .withColumn('ACTIVE_DAYS_LAG_5', F.lag('ACTIVE_DAYS_CURRENT', 5).over(w))
                       .withColumn('ACTIVE_DAYS_LAG_6', F.lag('ACTIVE_DAYS_CURRENT', 6).over(w))
                       .withColumn('ACTIVE_DAYS_LAG_7', F.lag('ACTIVE_DAYS_CURRENT', 7).over(w))
                       .withColumn('ACTIVE_DAYS_LAG_8', F.lag('ACTIVE_DAYS_CURRENT', 8).over(w))
                       .withColumn('ACTIVE_DAYS_LAG_12', F.lag('ACTIVE_DAYS_CURRENT', 12).over(w))
                       .withColumn('ACTIVE_DAYS_LAG_16', F.lag('ACTIVE_DAYS_CURRENT', 16).over(w))
                       .drop('TOTAL_DAYS', 'ACTIVE_DAYS_CURRENT')
                       .orderBy('STORE_ID', 'DP_NAME', 'YEARWEEK'))

conds = [
            data.STORE_ID == active_mapping.STORE_ID,
            data.DP_NAME == active_mapping.DP_NAME,
            data.YEARWEEK == active_mapping.YEARWEEK
        ]
data = data.join(active_mapping, conds, 'left').drop(active_mapping.STORE_ID, active_mapping.DP_NAME, active_mapping.YEARWEEK)

# COMMAND ----------

original_data = spark.sql('select * from sales_data')

# COMMAND ----------

category_mapping = original_data.select('DP_NAME', 'CATEGORY', 'SEGMENT', 'PACKSIZE').dropDuplicates().withColumnRenamed('DP_NAME', 'DP_NAME_x')

data = data.join(category_mapping, data.DP_NAME == category_mapping.DP_NAME_x, 'left').drop('DP_NAME_x')

# COMMAND ----------

location_mapping = (original_data.select('STORE_ID', 'STORE_NAME')
                                 .withColumn('STORE_CODE', F.substring('STORE_NAME', 1, 11))
                                 .drop('STORE_NAME')
                                 .dropDuplicates()
                                 .withColumnRenamed('STORE_ID', 'STORE_ID_x'))

data = (data.join(location_mapping, data.STORE_ID == location_mapping.STORE_ID_x, 'left')
            .withColumn('PROVINCE', F.substring('STORE_CODE', 5, 3))
            .withColumn('DISTRICT', F.substring('STORE_CODE', 9, 3))
            .drop('STORE_ID_x', 'STORE_CODE'))

# COMMAND ----------

data = (data.withColumn('DATE', F.from_unixtime('UNIX_TIMESTAMP').cast(types.DateType()))
            .withColumn('WEEK', F.col('YEARWEEK') % 100)
            .withColumn('YEAR', (F.col('YEARWEEK') - F.col('WEEK')) / 100)
            .withColumn('MONTH', F.month('DATE')))

# COMMAND ----------

# data.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DATA_WITH_TIME.parquet')
data = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DATA_WITH_TIME.parquet')

# COMMAND ----------

tmp = (original_data.select('STORE_ID', 'STORE_NAME', 'DP_NAME', 'CATEGORY', 'DATE', 'SALES_QTY')
                    .withColumn('PROVINCE', F.substring('STORE_NAME', 5, 3))
                    .withColumn('YEARWEEK', F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7))
                    .groupBy('STORE_ID', 'PROVINCE', 'DP_NAME', 'CATEGORY', 'YEARWEEK')
                    .agg(F.sum('SALES_QTY').alias('SALES_QTY')))

tmp = tmp.join(date_mapping, tmp.YEARWEEK == date_mapping.YEARWEEK_x, 'left').drop('YEARWEEK_x')

# COMMAND ----------

lag = 4
rolling = 12

w = (Window().partitionBy('STORE_ID', 'CATEGORY')
             .orderBy('UNIX_TIMESTAMP')
             .rangeBetween(-week(lag + rolling), -week(lag + 1)))

store_diversity = (tmp.withColumn('STORE_DIVERSITY', F.size(F.collect_set('DP_NAME').over(w)))
                                .select('STORE_ID', 'CATEGORY', 'YEARWEEK', 'STORE_DIVERSITY')
                                .dropDuplicates())

w = (Window().partitionBy('DP_NAME', 'PROVINCE')
             .orderBy('UNIX_TIMESTAMP')
             .rangeBetween(-week(lag + rolling), -week(lag + 1)))

product_diversity = (tmp.withColumn('PRODUCT_DIVERSITY', F.size(F.collect_set('STORE_ID').over(w)))
                        .select('DP_NAME', 'PROVINCE', 'YEARWEEK', 'PRODUCT_DIVERSITY')
                        .dropDuplicates())

# COMMAND ----------

store_diversity = (store_diversity.withColumnRenamed('STORE_ID', 'STORE_ID_x')
                                  .withColumnRenamed('CATEGORY', 'CATEGORY_x')
                                  .withColumnRenamed('YEARWEEK', 'YEARWEEK_x'))

conds = [
          data.STORE_ID == store_diversity.STORE_ID_x,
          data.CATEGORY == store_diversity.CATEGORY_x,
          data.YEARWEEK == store_diversity.YEARWEEK_x
        ]

data = (data.join(store_diversity, conds, 'left').drop('STORE_ID_x', 'CATEGORY_x', 'YEARWEEK_x'))

# COMMAND ----------

product_diversity = (product_diversity.withColumnRenamed('DP_NAME', 'DP_NAME_x')
                                      .withColumnRenamed('PROVINCE', 'PROVINCE_x')
                                      .withColumnRenamed('YEARWEEK', 'YEARWEEK_x'))

conds = [
          data.DP_NAME == product_diversity.DP_NAME_x,
          data.PROVINCE == product_diversity.PROVINCE_x,
          data.YEARWEEK == product_diversity.YEARWEEK_x
        ]

data = (data.join(product_diversity, conds, 'left').drop('DP_NAME_x', 'PROVINCE_x', 'YEARWEEK_x'))

# COMMAND ----------

data = data.na.fill(0)

# COMMAND ----------

store_performance = (data.select('STORE_ID', 'CATEGORY', 'YEARWEEK', 'R_SUM_4_4')
                         .groupBy('STORE_ID', 'CATEGORY', 'YEARWEEK')
                         .agg(
                              F.sum(F.col('R_SUM_4_4')).alias('STORE_PERFORMANCE')
                             )
                         .withColumnRenamed('STORE_ID', 'STORE_ID_x')
                         .withColumnRenamed('CATEGORY', 'CATEGORY_x')
                         .withColumnRenamed('YEARWEEK', 'YEARWEEK_x'))

product_performance = (data.select('DP_NAME', 'PROVINCE', 'YEARWEEK', 'R_SUM_4_4')
                           .groupBy('DP_NAME', 'PROVINCE', 'YEARWEEK')
                           .agg(
                                F.sum(F.col('R_SUM_4_4')).alias('PRODUCT_PERFORMANCE')
                               )
                           .withColumnRenamed('DP_NAME', 'DP_NAME_x')
                           .withColumnRenamed('PROVINCE', 'PROVINCE_x')
                           .withColumnRenamed('YEARWEEK', 'YEARWEEK_x'))

# COMMAND ----------

cond = [
        data.STORE_ID == store_performance.STORE_ID_x,
        data.CATEGORY == store_performance.CATEGORY_x,
        data.YEARWEEK == store_performance.YEARWEEK_x
       ]
data = data.join(store_performance, cond, 'left').drop('STORE_ID_x', 'CATEGORY_x', 'YEARWEEK_x')

cond = [
        data.DP_NAME == product_performance.DP_NAME_x,
        data.PROVINCE == product_performance.PROVINCE_x,
        data.YEARWEEK == product_performance.YEARWEEK_x
       ]
data = data.join(product_performance, cond, 'left').drop('DP_NAME_x', 'PROVINCE_x', 'YEARWEEK_x')

# COMMAND ----------

data = data.na.fill(0)

# COMMAND ----------

# data.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DATA_PERFORMANCE.parquet')
data = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DATA_PERFORMANCE.parquet')

# COMMAND ----------

promotion = promotion.withColumnRenamed('DP_NAME', 'DP_NAME_x').withColumnRenamed('YEARWEEK', 'YEARWEEK_x')

data = data.join(promotion, [data.DP_NAME == promotion.DP_NAME_x, data.YEARWEEK == promotion.YEARWEEK_x], 'left').drop('DP_NAME_x', 'YEARWEEK_x').na.fill(0)

# COMMAND ----------

calendar = calendar.withColumnRenamed('YEARWEEK', 'YEARWEEK_x')

data = data.join(calendar, data.YEARWEEK == calendar.YEARWEEK_x, 'left').drop('YEARWEEK_x').drop('STORE_NAME')

# COMMAND ----------

# data.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DATA_HOLIDAY.parquet')
data = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DATA_HOLIDAY.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # EDA

# COMMAND ----------

thresholds = [1, 2, 3, 4]
multipliers = [2, 3, 4, 5]

for threshold in thresholds:
  data = data.withColumn('OT_' + str(threshold), F.when(F.col('R_MEDIAN_4_4') < threshold, threshold).otherwise(F.col('R_MEDIAN_4_4')))

  for mul in multipliers:
    data = data.withColumn('IS_OL_' + str(threshold) + '_' + str(mul), F.when(F.col('SALES_QTY') / F.col('OT_' + str(threshold)) >= mul, True).otherwise(False))

outlier_pct = pd.DataFrame(columns = ['THRESHOLD', 'MULTIPLIER', 'RECORD_PCT', 'VOLUME_PCT'])

size = data.count()
total = data.agg(F.sum(F.col('SALES_QTY')).alias('TOTAL_SALES')).toPandas().values.tolist()[0][0]

for threshold in thresholds:
  for mul in multipliers:
    tmp = data.filter(F.col('IS_OL_' + str(threshold) + '_' + str(mul)))
    outlier_pct.loc[len(outlier_pct)] = [
                                          threshold,
                                          mul,
                                          (tmp.count() / size),
                                          (tmp.agg(F.sum(F.col('SALES_QTY')).alias('TOTAL_SALES')).toPandas().values.tolist()[0][0] / total)
                                        ]
  
outlier_pct

# COMMAND ----------

for threshold in thresholds:
  data = data.drop('OT_' + str(threshold))

  for mul in multipliers:
    data = data.drop('IS_OL_' + str(threshold) + '_' + str(mul))

# COMMAND ----------

data = (data.withColumn('TMP_MEDIAN', F.when(F.col('R_MEDIAN_4_4') < 2, 2).otherwise(F.col('R_MEDIAN_4_4')))
            .withColumn('PEAK', F.col('SALES_QTY') / F.col('TMP_MEDIAN'))
            .withColumn('IS_OUTLIER', F.when(F.col('PEAK') >= 3, True).otherwise(False))
            .drop('PEAK', 'TMP_MEDIAN'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Outlier Detection

# COMMAND ----------

time_threshold = 202340
num_months = 1

# COMMAND ----------

to_drop = (data.groupBy('STORE_ID', 'DP_NAME')
               .agg(F.max('YEARWEEK').alias('LAST_WEEK'))
               .withColumn('TO_DROP', F.when(time_threshold - F.col('LAST_WEEK') > 12, True).otherwise(False))
               .drop('LAST_WEEK')
               .withColumnRenamed('STORE_ID', 'STORE_ID_x')
               .withColumnRenamed('DP_NAME', 'DP_NAME_x'))

# COMMAND ----------

data = data.join(to_drop, [data.STORE_ID == to_drop.STORE_ID_x, data.DP_NAME == to_drop.DP_NAME_x], 'left').drop('STORE_ID_x', 'DP_NAME_x').filter(F.col('TO_DROP') == False).drop('TO_DROP')

# COMMAND ----------

to_drop.filter(F.col('TO_DROP') == True).select('STORE_ID_x', 'DP_NAME_x').dropDuplicates().count()

# COMMAND ----------

data.select('STORE_ID', 'DP_NAME').dropDuplicates().count()

# COMMAND ----------

28353 / 449215

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Label Rate Checking

# COMMAND ----------

data_df = data.toPandas()

# COMMAND ----------

report = pd.DataFrame()
report['SALES_QTY'] = [i for i in range(0, 11)]

for key, group in data_df.groupby(['YEARWEEK']):
  report[key] = group['SALES_QTY'].value_counts()

report

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Label Contribution Checking

# COMMAND ----------

# data.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DATA_ALL.parquet')
data = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/TEMP_FILE/231107_TMP_DATA_ALL.parquet')

# COMMAND ----------

data = data.withColumn('YEAR', F.col('YEAR').cast(types.IntegerType()).cast(types.StringType()))

# COMMAND ----------

time_threshold = 202340
num_months = 1

# COMMAND ----------

data = (data.withColumn('RAND_NUM', F.rand())
            .withColumn('ROW', F.row_number().over(Window().partitionBy('STORE_ID', 'DP_NAME', 'SALES_QTY').orderBy('RAND_NUM')))
            .drop('RAND_NUM')
            .orderBy('STORE_ID', 'DP_NAME', 'YEARWEEK'))

# COMMAND ----------

data_df = data.filter(F.col('YEARWEEK') >= 202310).toPandas()

data_df = data_df.drop(columns = ['UNIX_TIMESTAMP', 'DATE'])

# COMMAND ----------

cutoff_threshold = 5

train = data_df[(data_df['YEARWEEK'] < time_threshold - num_months * 4 * 2 + 1) & (data_df['ROW'] <= cutoff_threshold)].drop(columns = 'ROW').reset_index(drop = True)
val = data_df[(data_df['YEARWEEK'].between(time_threshold - num_months * 4 * 2 + 1, time_threshold - num_months * 4)) & (data_df['ROW'] <= cutoff_threshold)].drop(columns = 'ROW').reset_index(drop = True)
test = data_df[data_df['YEARWEEK'].between(time_threshold - num_months * 4 + 1, time_threshold)].drop(columns = 'ROW').reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # MODEL

# COMMAND ----------

model = cb.CatBoostRegressor(iterations = 100, depth = 9, learning_rate = 0.2, random_seed = 1204, thread_count = -1, loss_function = 'MAE')

cat_cols = ['STORE_ID', 'DP_NAME', 'YEARWEEK', 'CATEGORY', 'SEGMENT', 'PACKSIZE', 'PROVINCE', 'DISTRICT', 'WEEK', 'YEAR', 'MONTH',
            'YEARQUARTER', 'YEARHALF', 'HOLIDAYNAME', 'SPECIALEVENTNAME', 'WEEKTYPE', 'QUARTERTYPE']

X_train = train[train['IS_OUTLIER'] == False].drop(columns = ['SALES_QTY', 'IS_OUTLIER'])
y_train = train[train['IS_OUTLIER'] == False]['SALES_QTY']

X_val = val[val['IS_OUTLIER'] == False].drop(columns = ['SALES_QTY', 'IS_OUTLIER'])
y_val = val[val['IS_OUTLIER'] == False]['SALES_QTY']

X_test = test[test['IS_OUTLIER'] == False].drop(columns = ['SALES_QTY', 'IS_OUTLIER'])
y_test = test[test['IS_OUTLIER'] == False]['SALES_QTY']

# COMMAND ----------

model.fit(X_train, y_train, cat_features = cat_cols, eval_set = (X_val, y_val), plot = True)

# COMMAND ----------

pred = model.predict(X_test)
pred_actual = pred.copy()
pred = [round(i) for i in pred]

true = list(y_test)

# COMMAND ----------

def fa(y_true, y_pred):
  sum_err = sum([abs(pred - true) for true, pred in zip(y_true, y_pred)])
  sum_act = sum(y_true)

  return 1 - sum_err / sum_act

def fb(y_true, y_pred):
  sum_err = sum([pred - true for true, pred in zip(y_true, y_pred)])
  sum_act = sum(y_true)

  return sum_err / sum_act

# COMMAND ----------

pred_test = test[test['IS_OUTLIER'] == False].copy()
pred_test['PREDICTION'] = pred
pred_test = pred_test[['STORE_ID', 'PROVINCE', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY', 'PREDICTION', 'IS_OUTLIER']].sort_values(['STORE_ID', 'DP_NAME', 'YEARWEEK'])

pred_test

# COMMAND ----------

tmp_true = []
tmp_pred = []

for a, b in zip(true, pred):
  if a != 0:
    tmp_true.append(a)
    tmp_pred.append(b)

print('CatBoost Score - with Promotion - Zero Included:')
print(f"FA Score: {fa(true, pred):.2f}")
print(f"FB Score: {fb(true, pred):.2f}")

print()

print('CatBoost Score - with Promotion - Zero Excluded:')
print(f"FA Score: {fa(tmp_true, tmp_pred):.2f}")
print(f"FB Score: {fb(tmp_true, tmp_pred):.2f}")

# COMMAND ----------

fi = pd.DataFrame()
fi['FEATURE'] = X_train.columns
fi['IMPORTANCE'] = model.feature_importances_

fi.sort_values('IMPORTANCE', ascending = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Accuracy - Total - Zero Excluded

# COMMAND ----------

pred_test = test[test['IS_OUTLIER'] == False].copy()
pred_test['PREDICTION'] = pred
pred_test = pred_test[['STORE_ID', 'PROVINCE', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY', 'PREDICTION', 'IS_OUTLIER']].sort_values(['STORE_ID', 'DP_NAME', 'YEARWEEK'])
pred_test['SALES_SEGMENT'] = np.where(pred_test['SALES_QTY'] < 20, pred_test['SALES_QTY'], '>= 20')

col = 'YEARWEEK'

conds = [
          pred_test[col] == time_threshold - 3,
          pred_test[col] == time_threshold - 2,
          pred_test[col] == time_threshold - 1,
          pred_test[col] == time_threshold,
        ]

choices = [
            'W1', 'W2', 'W3', 'W4'
          ]

pred_test['WEEK_SEGMENT'] = np.select(conds, choices, default = None)

pred_test = pred_test[~pred_test['WEEK_SEGMENT'].isna()]

pred_test['FA_ERR'] = pred_test.apply(lambda x: abs(x['SALES_QTY'] - x['PREDICTION']), axis = 1)
pred_test['FA_ERR'] = np.where(pred_test['SALES_QTY'] == 0, 0, pred_test['FA_ERR'])

# COMMAND ----------

col = 'CATEGORY'

report = pred_test.groupby([col, 'WEEK_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = report.apply(lambda x: 1 - (x['FA_ERR'] / x['SALES_QTY']) if x['SALES_QTY'] != 0 else None, axis = 1)
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report = report.pivot(index = col, columns = 'WEEK_SEGMENT', values = 'FA')
report.reset_index(inplace = True)

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri.reset_index(inplace = True)

report.merge(contri, on = col, how = 'left')

# COMMAND ----------

col = 'SALES_SEGMENT'

report = pred_test.groupby([col, 'WEEK_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = report.apply(lambda x: 1 - (x['FA_ERR'] / x['SALES_QTY']) if x['SALES_QTY'] != 0 else None, axis = 1)
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report = report.pivot(index = col, columns = 'WEEK_SEGMENT', values = 'FA')
report.reset_index(inplace = True)

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri.reset_index(inplace = True)

report = report.merge(contri, on = col, how = 'left')
report = report.loc[pd.to_numeric(report[col], errors = 'coerce').sort_values().index].set_index(col)

report

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Accuracy - Total - Zero Included

# COMMAND ----------

pred_test = test[test['IS_OUTLIER'] == False].copy()
pred_test['PREDICTION'] = pred
pred_test = pred_test[['STORE_ID', 'PROVINCE', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY', 'PREDICTION', 'IS_OUTLIER']].sort_values(['STORE_ID', 'DP_NAME', 'YEARWEEK'])
pred_test['SALES_SEGMENT'] = np.where(pred_test['SALES_QTY'] < 20, pred_test['SALES_QTY'], '>= 20')

col = 'YEARWEEK'

conds = [
          pred_test[col] == time_threshold - 3,
          pred_test[col] == time_threshold - 2,
          pred_test[col] == time_threshold - 1,
          pred_test[col] == time_threshold,
        ]

choices = [
            'W1', 'W2', 'W3', 'W4'
          ]

pred_test['WEEK_SEGMENT'] = np.select(conds, choices, default = None)

pred_test = pred_test[~pred_test['WEEK_SEGMENT'].isna()]

pred_test['FA_ERR'] = pred_test.apply(lambda x: abs(x['SALES_QTY'] - x['PREDICTION']), axis = 1)

# COMMAND ----------

col = 'CATEGORY'

report = pred_test.groupby([col, 'WEEK_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = report.apply(lambda x: 1 - (x['FA_ERR'] / x['SALES_QTY']) if x['SALES_QTY'] != 0 else None, axis = 1)
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report = report.pivot(index = col, columns = 'WEEK_SEGMENT', values = 'FA')
report.reset_index(inplace = True)

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri.reset_index(inplace = True)

report.merge(contri, on = col, how = 'left')

# COMMAND ----------

col = 'SALES_SEGMENT'

report = pred_test.groupby([col, 'WEEK_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = report.apply(lambda x: 1 - (x['FA_ERR'] / x['SALES_QTY']) if x['SALES_QTY'] != 0 else None, axis = 1)
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report = report.pivot(index = col, columns = 'WEEK_SEGMENT', values = 'FA')
report.reset_index(inplace = True)

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri.reset_index(inplace = True)

report = report.merge(contri, on = col, how = 'left')
report = report.loc[pd.to_numeric(report[col], errors = 'coerce').sort_values().index].set_index(col)

report

# COMMAND ----------

pred_test = test[test['IS_OUTLIER'] == False].copy()
pred_test['PREDICTION'] = pred
pred_test['PREDICTION_ACT'] = pred_actual

pred_test = pred_test[['STORE_ID', 'PROVINCE', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY', 'PREDICTION', 'PREDICTION_ACT']]

pred_test

# COMMAND ----------

tmp_train = train.copy()
tmp_train['ID'] = tmp_train['STORE_ID'].astype('str') + '|' + tmp_train['DP_NAME']
pred_test['ID'] = pred_test['STORE_ID'].astype('str') + '|' + pred_test['DP_NAME']

tmp_pred_test = pred_test[~pred_test['ID'].isin(tmp_train['ID'].unique().tolist())]

# COMMAND ----------

tmp_pred_test['SEGMENT'] = np.where(tmp_pred_test['SALES_QTY'] < 20, tmp_pred_test['SALES_QTY'], '>= 20')

tmp = tmp_pred_test.copy()

tmp['NEG_BIAS'] = np.where(tmp['PREDICTION'] - tmp['SALES_QTY'] < 0, tmp['PREDICTION'] - tmp['SALES_QTY'], 0)
tmp['POS_BIAS'] = np.where(tmp['PREDICTION'] - tmp['SALES_QTY'] > 0, tmp['PREDICTION'] - tmp['SALES_QTY'], 0)

data_report = tmp.groupby('SEGMENT').agg(
                                          TOTAL_RECORDS = ('SALES_QTY', 'count'),
                                          TOTAL_ACTUAL_VOL = ('SALES_QTY', 'sum'),
                                          AVG_ACTUAL_VOL = ('SALES_QTY', 'mean'),
                                          AVG_PREDICT_VOL = ('PREDICTION', 'mean'),
                                          TOTAL_NEGATIVE_BIAS = ('NEG_BIAS', 'sum'),
                                          TOTAL_POSITIVE_BIAS = ('POS_BIAS', 'sum')
                                         )

data_report['TOTAL_CONTRIBUTION'] = data_report['TOTAL_ACTUAL_VOL'] / tmp_pred_test['SALES_QTY'].sum()

data_report.reset_index(inplace = True)

data_report = data_report[['SEGMENT', 'TOTAL_RECORDS', 'TOTAL_ACTUAL_VOL', 'TOTAL_CONTRIBUTION',
                           'AVG_ACTUAL_VOL', 'AVG_PREDICT_VOL', 'TOTAL_NEGATIVE_BIAS', 'TOTAL_POSITIVE_BIAS']]

data_report = data_report.loc[pd.to_numeric(data_report['SEGMENT'], errors = 'coerce').sort_values().index].set_index('SEGMENT')

data_report

# COMMAND ----------

tmp = tmp_pred_test.copy()

tmp['FA_SCORE'] = abs(tmp['PREDICTION'] - tmp['SALES_QTY'])
tmp['FB_SCORE'] = tmp['PREDICTION'] - tmp['SALES_QTY']

tmp = tmp.groupby('SEGMENT').agg(SUM_ACTUAL = ('SALES_QTY', 'sum'),
                                 COUNT_ACTUAL = ('SALES_QTY', 'count'),
                                 FA_SCORE = ('FA_SCORE', 'sum'),
                                 FB_SCORE = ('FB_SCORE', 'sum'))

tmp['FA_SCORE'] = tmp.apply(lambda x: 1 - (x['FA_SCORE'] / x['SUM_ACTUAL']) if x['SUM_ACTUAL'] != 0 else None, axis = 1)
tmp['FB_SCORE'] = tmp.apply(lambda x: x['FB_SCORE'] / x['COUNT_ACTUAL'] if x['COUNT_ACTUAL'] != 0 else None, axis = 1)

tmp.reset_index(inplace = True)

tmp = tmp.loc[pd.to_numeric(tmp['SEGMENT'], errors = 'coerce').sort_values().index].set_index('SEGMENT').rename(columns = {'SUM_ACTUAL': 'TOTAL_ACTUAL_VOL'}).drop(columns = ['COUNT_ACTUAL'])

tmp

# COMMAND ----------

pred_test['SEGMENT'] = np.where(pred_test['SALES_QTY'] < 20, pred_test['SALES_QTY'], '>= 20')

tmp = pred_test.copy()

tmp['NEG_BIAS'] = np.where(tmp['PREDICTION'] - tmp['SALES_QTY'] < 0, tmp['PREDICTION'] - tmp['SALES_QTY'], 0)
tmp['POS_BIAS'] = np.where(tmp['PREDICTION'] - tmp['SALES_QTY'] > 0, tmp['PREDICTION'] - tmp['SALES_QTY'], 0)

data_report = tmp.groupby('SEGMENT').agg(
                                          TOTAL_RECORDS = ('SALES_QTY', 'count'),
                                          TOTAL_ACTUAL_VOL = ('SALES_QTY', 'sum'),
                                          AVG_ACTUAL_VOL = ('SALES_QTY', 'mean'),
                                          AVG_PREDICT_VOL = ('PREDICTION', 'mean'),
                                          TOTAL_NEGATIVE_BIAS = ('NEG_BIAS', 'sum'),
                                          TOTAL_POSITIVE_BIAS = ('POS_BIAS', 'sum')
                                         )

data_report['TOTAL_CONTRIBUTION'] = data_report['TOTAL_ACTUAL_VOL'] / pred_test['SALES_QTY'].sum()

data_report.reset_index(inplace = True)

data_report = data_report[['SEGMENT', 'TOTAL_RECORDS', 'TOTAL_ACTUAL_VOL', 'TOTAL_CONTRIBUTION',
                           'AVG_ACTUAL_VOL', 'AVG_PREDICT_VOL', 'TOTAL_NEGATIVE_BIAS', 'TOTAL_POSITIVE_BIAS']]

data_report = data_report.loc[pd.to_numeric(data_report['SEGMENT'], errors = 'coerce').sort_values().index].set_index('SEGMENT')

data_report

# COMMAND ----------

tmp = pred_test.copy()

tmp['FA_SCORE'] = abs(tmp['PREDICTION'] - tmp['SALES_QTY'])
tmp['FB_SCORE'] = tmp['PREDICTION'] - tmp['SALES_QTY']

tmp = tmp.groupby('SEGMENT').agg(SUM_ACTUAL = ('SALES_QTY', 'sum'),
                                 COUNT_ACTUAL = ('SALES_QTY', 'count'),
                                 FA_SCORE = ('FA_SCORE', 'sum'),
                                 FB_SCORE = ('FB_SCORE', 'sum'))

tmp['FA_SCORE'] = tmp.apply(lambda x: 1 - (x['FA_SCORE'] / x['SUM_ACTUAL']) if x['SUM_ACTUAL'] != 0 else None, axis = 1)
tmp['FB_SCORE'] = tmp.apply(lambda x: x['FB_SCORE'] / x['COUNT_ACTUAL'] if x['COUNT_ACTUAL'] != 0 else None, axis = 1)

tmp.reset_index(inplace = True)

tmp = tmp.loc[pd.to_numeric(tmp['SEGMENT'], errors = 'coerce').sort_values().index].set_index('SEGMENT').rename(columns = {'SUM_ACTUAL': 'TOTAL_ACTUAL_VOL'}).drop(columns = ['COUNT_ACTUAL'])

tmp

# COMMAND ----------

pred_test = test[test['IS_OUTLIER'] == False].copy()
pred_test['PREDICTION'] = pred
pred_test = pred_test[['STORE_ID', 'PROVINCE', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY', 'PREDICTION', 'IS_OUTLIER']].sort_values(['STORE_ID', 'DP_NAME', 'YEARWEEK'])
pred_test['SALES_SEGMENT'] = np.where(pred_test['SALES_QTY'] < 20, pred_test['SALES_QTY'], '>= 20')

# COMMAND ----------

col = 'YEARWEEK'

conds = [
          pred_test[col] == time_threshold - 3,
          pred_test[col] == time_threshold - 2,
          pred_test[col] == time_threshold - 1,
          pred_test[col] == time_threshold,
        ]

choices = [
            'W1', 'W2', 'W3', 'W4'
          ]

pred_test['WEEK_SEGMENT'] = np.select(conds, choices, default = None)

# COMMAND ----------

pred_test = pred_test[~pred_test['WEEK_SEGMENT'].isna()]

# COMMAND ----------

pred_test['FA_ERR'] = pred_test.apply(lambda x: abs(x['SALES_QTY'] - x['PREDICTION']), axis = 1)
pred_test['FA_ERR'] = np.where(pred_test['SALES_QTY'] == 0, 0, pred_test['FA_ERR'])

# COMMAND ----------

col = 'CATEGORY'

report = pred_test.groupby([col, 'WEEK_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = report.apply(lambda x: 1 - (x['FA_ERR'] / x['SALES_QTY']) if x['SALES_QTY'] != 0 else None, axis = 1)
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report.pivot(index = col, columns = 'WEEK_SEGMENT', values = 'FA')

# COMMAND ----------

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri

# COMMAND ----------

col = 'SALES_SEGMENT'

report = pred_test.groupby([col, 'WEEK_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = report.apply(lambda x: 1 - (x['FA_ERR'] / x['SALES_QTY']) if x['SALES_QTY'] != 0 else None, axis = 1)
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report = report.pivot(index = col, columns = 'WEEK_SEGMENT', values = 'FA')
report.reset_index(inplace = True)
report = report.loc[pd.to_numeric(report['SALES_SEGMENT'], errors = 'coerce').sort_values().index].set_index('SALES_SEGMENT')

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri.reset_index(inplace = True)
contri = contri.loc[pd.to_numeric(contri['SALES_SEGMENT'], errors = 'coerce').sort_values().index].set_index('SALES_SEGMENT')

report[contri.columns] = contri[contri.columns]

report

# COMMAND ----------

col = 'PROVINCE'

report = pred_test.groupby([col, 'WEEK_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = report.apply(lambda x: 1 - (x['FA_ERR'] / x['SALES_QTY']) if x['SALES_QTY'] != 0 else None, axis = 1)
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report = report.pivot(index = col, columns = 'WEEK_SEGMENT', values = 'FA')

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

report[contri.columns] = contri[contri.columns]

report

# COMMAND ----------

pred_test.drop(columns = ['IS_OUTLIER', 'SALES_SEGMENT', 'WEEK_SEGMENT', 'FA_ERR']).display()

# COMMAND ----------

store_ids = [1531, 1610, 2012, 2024, 1696, 2130, 2111, 1861, 2192, 2835, 3322, 3399,
             3612, 3673, 4498, 6673, 6835, 7670, 8917, 9287, 10051, 10110, 6370]

# COMMAND ----------

pred_test[pred_test['STORE_ID'].isin(store_ids)].drop(columns = ['IS_OUTLIER', 'SALES_SEGMENT', 'WEEK_SEGMENT', 'FA_ERR']).display()

# COMMAND ----------

spark.createDataFrame(pred_test.drop(columns = ['IS_OUTLIER', 'SALES_SEGMENT', 'WEEK_SEGMENT', 'FA_ERR'])).write.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/231114_RESULT_SEP.parquet')

# COMMAND ----------

# MAGIC %fs ls /mnt/adls/BHX_FC/FBFC/WEEKLY/

# COMMAND ----------

spark.read.parquet('/mnt/adls/BHX_FC/FBFC/WEEKLY/231114_RESULT_SEP.parquet').display()

# COMMAND ----------

