# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %run ./nb_common_function

# COMMAND ----------

shows_raw_sdf = spark.read.option('multiline', 'true').json(
 f'{RAW_PATH}/shows/shows_{RUN_DATE}.json'
)

bronze_shows = shows_raw_sdf.select(
 col('id').cast('int').alias('show_id'),
 col('name').alias('show_name'),
 col('type'),
 col('language'),
 col('status'),
 col('premiered').alias('premiere_date'),
 col('ended').alias('end_date'),
 col('rating.average').alias('rating'),
 col('network.name').alias('network_name'),
 col('network.country.name').alias('network_country'),
 col('webChannel.name').alias('web_channel_name'),
 col('genres'),
 col('summary'),
 current_timestamp().alias('ingested_at')
)

# Check for errors in DataFrame
display(bronze_shows)

# COMMAND ----------

#write shows to delta table
write_delta_table(bronze_shows, f'{bronze_schema_name}.bronze_shows')

# COMMAND ----------

eps_raw_sdf = spark.read.option('multiline', 'true').json(
 f'{RAW_PATH}/episodes/episodes_{RUN_DATE}.json'
)
bronze_episodes = eps_raw_sdf.select(
 col('id').cast('int').alias('episode_id'),
 col('show_id').cast('int'),
 col('name').alias('episode_name'),
 col('season').cast('int'),
 col('number').cast('int').alias('episode_number'),
 col('airdate').alias('air_date'),
 col('runtime').cast('int'),
 col('rating.average').alias('rating'),
 col('summary'),
 current_timestamp().alias('ingested_at')
)


# Check for errors in DataFrame
display(bronze_episodes)

# COMMAND ----------

#write episodes to delta table
write_delta_table(bronze_episodes, f'{bronze_schema_name}.bronze_episodes')

# COMMAND ----------

# DBTITLE 1,Cell 6
cast_raw_sdf = spark.read.option('multiline', 'true').json(
 f'{RAW_PATH}/cast/cast_{RUN_DATE}.json'
)
bronze_cast = cast_raw_sdf.select(
 col('show_id').cast('int'),
 col('person.id').cast('int').alias('person_id'),
 col('person.name').alias('person_name'),
 col('person.gender'),
 col('person.birthday'),
 col('person.country.name').alias('person_country'),
 col('character.id').cast('int').alias('character_id'),
 col('character.name').alias('character_name'),
 col('self').cast('boolean'),
 col('voice').cast('boolean'),
 current_timestamp().alias('ingested_at')
)

# Check for errors in DataFrame
display(bronze_cast)


# COMMAND ----------

#write episodes to delta table
write_delta_table(bronze_cast, f'{bronze_schema_name}.bronze_cast')

# COMMAND ----------

# Attempt to write a DataFrame with an extra column → will FAIL by default
from pyspark.sql import Row
bad_data = spark.createDataFrame([
 Row(show_id=9999, show_name='Test Show', extra_col='bad_field')
])
try:
 bad_data.write.format('delta').mode('append') \
 .saveAsTable(f'{bronze_schema_name}.bronze_shows')
except Exception as e:
 print(f'Schema enforcement caught: {type(e).__name__}')
 print('Delta rejected the write – schema mismatch detected!')


# COMMAND ----------

spark.sql(f'DESCRIBE {bronze_schema_name}.bronze_shows').display()


# COMMAND ----------

# Allow schema evolution with mergeSchema
new_shows_data = spark.createDataFrame([
 Row(show_id=1001, show_name='New Show', new_column='extra_value')
])

# Cast show_id to match the existing table's IntegerType
new_shows_data = new_shows_data.withColumn('show_id', col('show_id').cast(IntegerType()))

new_shows_data.write \
 .format('delta') \
 .mode('append') \
 .option('mergeSchema', 'true') \
 .saveAsTable(f'{bronze_schema_name}.bronze_shows')
print('Schema evolved successfully')
display(spark.sql(f'DESCRIBE {bronze_schema_name}.bronze_shows'))