# Databricks notebook source
import requests, json, time , os
from datetime import datetime
from pyspark.sql.functions import (
    col, explode_outer, to_date, current_timestamp, coalesce, lit, trim,
    regexp_replace, when, broadcast, rand, floor, explode, array ,count, avg, round, countDistinct,
    row_number, dense_rank, desc,sum 
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("USE CATALOG tvmaze_adls_dev")

# Flatten selected fields (shows have nested _links, network, etc.)

RUN_DATE = datetime.now().strftime('%Y%m%d')

# COMMAND ----------

# catalog = 'tvmaze_adls_dev'
bronze_schema_name = 'bronze'
silver_schema_name = 'silver'
gold_schema_name = 'gold'
BASE_URL = 'https://api.tvmaze.com'
RAW_PATH  = 'abfss://tvmaze@tvmazeadls001dev.dfs.core.windows.net/raw/'

# COMMAND ----------

def save_json_to_adls(data, path):
    """Write list-of-dicts as JSON using dbutils.fs.put"""
    json_str = json.dumps(data, indent=2)
    dbutils.fs.put(path, json_str, overwrite=True)
    print(f'Saved {len(data)} records to {path}')

# COMMAND ----------

def write_delta_table(df, table_name, mode='overwrite', overwrite_schema=True):
    df.write \
      .format('delta') \
      .mode(mode) \
      .option('overwriteSchema', str(overwrite_schema).lower()) \
      .saveAsTable(table_name)
    if spark.catalog.tableExists(table_name):
        print(f'{table_name} created')
    else:
        print(f'{table_name} NOT created')
    display(spark.sql(f'DESCRIBE EXTENDED {table_name}'))

# COMMAND ----------

def save_silver(df, name, partition_by=None):
    w = df.write.format('delta').mode('overwrite').option('overwriteSchema','true')
    # If partition_by is specified, partition the table by that column
    if partition_by: w = w.partitionBy(partition_by)
    # Save the DataFrame as a Delta table in the silver schema
    w.saveAsTable(f'{silver_schema_name}.{name}')
    # Print the table name and row count after saving
    print(name, spark.table(f'{silver_schema_name}.{name}').count())
