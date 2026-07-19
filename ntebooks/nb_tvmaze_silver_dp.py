# Databricks notebook source
# MAGIC %run ./nb_common_function

# COMMAND ----------


bz_shows = spark.table(f'{catalog}.{bronze_schema_name}.bronze_shows')
silver_shows = (bz_shows.select(
        col('show_id'),
        trim(col('show_name')).alias('show_name'),
        col('language'),
        col('type'),
        col('status'),
        to_date('premiere_date').alias('premiere_date'),
        to_date('end_date').alias('end_date'),
        col('rating').cast('double').alias('rating'),
        col('network_name'),
        col('web_channel_name'),
        col('network_country'),
        # strip HTML tags from summary
        regexp_replace(col('summary'), '<[^>]+>', '').alias('summary'),
        col('genres'))                       # keep array for normalization below
    # ---- handle nulls / standardize ---
    .withColumn('language', coalesce(col('language'), lit('Unknown')))
    .withColumn('rating',   coalesce(col('rating'),   lit(0.0)))
    .withColumn('network_name',\
        coalesce(col('network_name'), col('web_channel_name'), lit('N/A'))))

# ---- normalize genres into rows (array -> one row per genre) ---
silver_show_genres = (silver_shows
    .select('show_id', explode_outer('genres').alias('genre'))
    .withColumn('genre', coalesce(col('genre'), lit('Unspecified'))))
silver_shows = silver_shows.drop('genres')

# COMMAND ----------

bz_eps  = spark.table(f'{catalog}.{bronze_schema_name}.bronze_episodes')
bz_cast = spark.table(f'{catalog}.{bronze_schema_name}.bronze_cast')
silver_episodes = (bz_eps.select(
        col('episode_id'),
        col('show_id'),
        trim(col('episode_name')).alias('episode_name'),
        col('season'),
        col('episode_number'),
        to_date('air_date').alias('air_date'),
        col('runtime'),
        col('rating').cast('double').alias('episode_rating')))

silver_cast = (bz_cast.select(
        col('show_id'),
        col('person_id'),
        trim(col('person_name')).alias('cast_name'),
        col('gender'),
        col('person_country').alias('cast_country'),
        col('character_id'),
        trim(col('character_name')).alias('character_name')))


# COMMAND ----------


save_silver(silver_shows,       'silver_shows')
save_silver(silver_show_genres, 'silver_show_genres')
save_silver(silver_episodes,    'silver_episodes', partition_by='season')
save_silver(silver_cast,        'silver_cast')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## fact_show_data

# COMMAND ----------

shows  = spark.table(f'{catalog}.{silver_schema_name}.silver_shows')
genres = spark.table(f'{catalog}.{silver_schema_name}.silver_show_genres')
eps    = spark.table(f'{catalog}.{silver_schema_name}.silver_episodes')
cast   = spark.table(f'{catalog}.{silver_schema_name}.silver_cast')
fact_show_data = (eps.alias('e')
    # shows & cast are small dimensions -> BROADCAST to avoid shuffles
    .join(broadcast(shows.alias('s')),  col('e.show_id') == col('s.show_id'))
    .join(genres.alias('g'), col('e.show_id') == col('g.show_id'))
    .join(broadcast(cast.alias('c')),col('e.show_id') == col('c.show_id'))
    .select(
        col('e.show_id'), col('s.show_name'), col('s.language'),
        col('g.genre'), col('e.season'), col('e.episode_name'),
        col('e.air_date'), col('e.runtime'),
        col('c.cast_name'), col('c.character_name')))

# Save as MANAGED table in Unity / Spark Catalog (deliverable)
fact_show_data.write.format('delta').mode('overwrite')\
    .option('overwriteSchema','true')\
    .partitionBy('language')\
    .saveAsTable(f'{catalog}.{silver_schema_name}.fact_show_data')
    
print('fact rows:', spark.table(f'{catalog}.{silver_schema_name}.fact_show_data').count())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## skew / salting

# COMMAND ----------

# DBTITLE 1,Cell 8
# Problem: a few shows (e.g. long-running soaps) have 1000s of episodes+cast rows;
# joining eps<->cast on show_id sends all their rows to one task (skew).

SALT = 8
eps_salted  = eps.withColumn('salt', floor(rand() * SALT))          # big side
# Add a 'salt' column to the cast DataFrame by exploding an array of SALT values (0 to SALT-1).
# This creates SALT duplicate rows for each original row, each with a different salt value.
cast_salted = cast.withColumn('salt',
        explode(array(*[lit(i) for i in range(SALT)])))


joined = eps_salted.join(cast_salted, ['show_id', 'salt']).drop('salt')
# Each hot show_id now spreads across 8 tasks instead of 1.




# COMMAND ----------



# COMMAND ----------

# Modern alternative also enabled (AQE skew handling):
# spark.conf.set('spark.sql.adaptive.enabled', 'true')
# spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Delta OPTIMIZE + ZORDER

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %sql
# MAGIC -- OPTIMIZE and ZORDER syntax for Databricks SQL
# MAGIC OPTIMIZE tvmaze_adls_dev.silver.fact_show_data ZORDER BY (show_id, season);
# MAGIC OPTIMIZE tvmaze_adls_dev.silver.silver_episodes ZORDER BY (show_id);
# MAGIC VACUUM tvmaze_adls_dev.silver.fact_show_data RETAIN 168 HOURS;