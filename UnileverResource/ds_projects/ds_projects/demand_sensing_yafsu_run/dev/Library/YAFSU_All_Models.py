# Databricks notebook source
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

# MAGIC %run "./SimpleLagModels"

# COMMAND ----------

# MAGIC %run "./NixtlaModels"

# COMMAND ----------

MODEL_DIRECTORY = {
    'SimpleLagModels': simple_lag_models,
    # 'NixtlaUnivariateModels': nixtla_forecast_univariate_models,
}

# COMMAND ----------



# COMMAND ----------

@ray.remote
def REMOTE_YAFSU_model(model_name, key, df_group, target_var, exo_vars=[], categorical_vars=[]):
    model_function = MODEL_DIRECTORY[model_name]
    return model_function(key, df_group, target_var, exo_vars, categorical_vars)

# COMMAND ----------

def YAFSU_solution_run(df, target_var, exo_vars=[], categorical_vars=[]):
    forecast_dict = {}
    for model_name in MODEL_DIRECTORY.keys():
        tasks = [REMOTE_YAFSU_model.remote(model_name, key, df_group, target_var, exo_vars, categorical_vars) for key, df_group in df.groupby('KEY')]
        tasks = ray.get(tasks)
        result = pd.concat(tasks)
        forecast_dict[model_name] = result
    return forecast_dict