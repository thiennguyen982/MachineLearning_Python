# Databricks notebook source
import pandas as pd
from time import time
import ray


# COMMAND ----------

# MAGIC %md
# MAGIC # Common Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Related Excel functions

# COMMAND ----------



# COMMAND ----------

def read_excel_file(file_path, sheet_name, skiprows=0):
    print(f"{file_path} - Sheet {sheet_name}")
    df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skiprows)
    return df


@ray.remote
def REMOTE_read_excel_file(file_path, sheet_name, skiprows=0):
    return read_excel_file(file_path, sheet_name, skiprows)


def read_excel_folder(folder_path, sheet_name, skiprows=0):
    folder_items = os.listdir(folder_path)
    tasks = [
        REMOTE_read_excel_file.remote(
            folder_path + "/" + file_item, sheet_name, skiprows
        )
        for file_item in folder_items
    ]
    tasks = ray.get(tasks)
    result = pd.concat(tasks)
    return result

def write_excel_file(df, file_name, sheet_name, dbfs_directory):
    df.to_excel(f"/tmp/{file_name}.xlsx", index=False, sheet_name=sheet_name)
    shutil.copy(f"/tmp/{file_name}.xlsx", dbfs_directory + f"/{file_name}.xlsx")


@ray.remote
def REMOTE_write_excel_file(df, file_name, sheet_name, dbfs_directory):
    write_excel_file(df, file_name, sheet_name, dbfs_directory)
    return 1


def write_excel_dataframe(df_all, groupby_arr, sheet_name, dbfs_directory):
    tasks = []
    for key, df_group in df_all.groupby(groupby_arr):
        if type(key) != str:
            file_name = "_".join(key)
        else:
            file_name = key
        tasks.append(
            REMOTE_write_excel_file.remote(
                df_group, file_name, sheet_name, dbfs_directory
            )
        )
    tasks = ray.get(tasks)

# COMMAND ----------

def find_yearweek_diff(yearweek, diff):
    yearweek_arr = [
        *range(202001, 202054),
        *range(202101, 202152),
        *range(202201, 202253),
        *range(202301, 202353),
        *range(202401, 202453),
    ]
    index = yearweek_arr.index(yearweek)
    return yearweek_arr[index - diff]

# COMMAND ----------

def filename_standardize(filename):
    import re

    filename = re.sub("[^A-Za-z0-9-]+", "_", filename)
    return filename

# COMMAND ----------

# MAGIC %md
# MAGIC # YAFSU Wrapper

# COMMAND ----------

def with_fail_safe(func):
    import time
    import pandas as pd
    import traceback

    def wrapper(*args, **kwargs):
        begin = time.time()

        key = kwargs.get("key_ts", "NO KEY")
        model_name = func.__name__

        output_object = {
            "KEY_TS": key,
            "MODEL_NAME": model_name,
            "ERROR": "NO ERROR",
            "STACKTRACE": "NO ERROR",
        }

        try:
            output_object.update({"OUTPUT": func(*args, **kwargs)})
        except Exception as ex:
            output_object.update(
                {
                    "ERROR": str(ex),
                    "STACKTRACE": str(traceback.format_exc()),
                    "OUTPUT": pd.DataFrame(),
                }
            )

        output_object["RUNNING_TIME"] = round(time.time() - begin, 5)
        return output_object

    return wrapper