# Databricks notebook source
def generate_fe(column, partition_by, lag_num, prefix: str = ""):
  partition_key = ", ".join(partition_by)
  query = f"""
sum({column}) over (partition by {partition_key}  order by unix_date(ds) range between {lag_num} preceding and ${{LAG}}*3 preceding) as rolling_sum_{prefix}{column}_d{lag_num},
avg({column}) over (partition by {partition_key} order by unix_date(ds) range between {lag_num} preceding and ${{LAG}}*3 preceding) as rolling_avg_{prefix}{column}_d{lag_num},
min({column}) over (partition by {partition_key} order by unix_date(ds) range between {lag_num} preceding and ${{LAG}}*3 preceding) as rolling_min_{prefix}{column}_d{lag_num},
max({column}) over (partition by {partition_key} order by unix_date(ds) range between {lag_num} preceding and ${{LAG}}*3 preceding) as rolling_max_{prefix}{column}_d{lag_num},
std({column}) over (partition by {partition_key} order by unix_date(ds) range between {lag_num} preceding and ${{LAG}}*3 preceding) as rolling_std_{prefix}{column}_d{lag_num},
sum(case when {column} > 0 then 1 end) over (partition by {partition_key} order by unix_date(ds) range between {lag_num} preceding and ${{LAG}}*3 preceding) as rolling_count_{prefix}{column}_d{lag_num},"""
  return query

print(generate_fe(
  column = "tts",
  partition_by= ["banner", "region", "dp_name"],
  lag_num = 180
))


# COMMAND ----------

lst_gen = [
  {
    "column": "est_demand",
    "partition_by": ["banner", "region", "dp_name"],
    "prefix": ""
  },
  {
    "column": "est_demand",
    "partition_by": ["banner", "region", "dp_name", "half_week"],
    "prefix": "half_week_"
  },
  {
    "column": "est_demand",
    "partition_by": ["banner", "region", "category"],
    "prefix": "category_"
  },
  {
    "column": "actual_sale",
    "partition_by": ["banner", "region", "dp_name"],
    "prefix": ""
  },
  {
    "column": "tts",
    "partition_by": ["banner", "region", "dp_name"],
    "prefix": ""
  },
]
for day in [14, 28, 90, 180, 365]:
  for gen in lst_gen:
    print(generate_fe(
      column = gen["column"],
      partition_by= gen["partition_by"],
      lag_num = day,
      prefix = gen["prefix"]
    ))

# COMMAND ----------

