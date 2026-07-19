# Databricks notebook source
# MAGIC %run ./nb_common_function

# COMMAND ----------

fact = spark.table(f'{silver_schema_name}.fact_show_data')
eps  = spark.table(f'{silver_schema_name}.silver_episodes')
cast = spark.table(f'{silver_schema_name}.silver_cast')
genres = spark.table(f'{silver_schema_name}.silver_show_genres')

# 1. Episodes per season (grouping + aggregation)
gold_eps_per_season = (eps.groupBy('show_id', 'season')
    .agg(count('episode_id').alias('episode_count'),
         round(avg('runtime'), 1).alias('avg_runtime_min')))

# 2. Avg runtime per show + rank across shows (window function)
gold_avg_runtime = eps.groupBy('show_id')\
    .agg(round(avg('runtime'), 1).alias('avg_runtime'))

gold_avg_runtime = gold_avg_runtime.withColumn(
    'runtime_rank',
    dense_rank().over(Window.orderBy(col('avg_runtime').desc()))
)

# COMMAND ----------

#3. Top cast members = appear in most shows (window: rank)
w_cast = Window.orderBy(desc('show_count'))
gold_top_cast = cast.groupBy('person_id', 'cast_name')\
    .agg(countDistinct('show_id').alias('show_count'))\
    .withColumn('rank', dense_rank().over(w_cast))\
    .where('rank <= 20')

# 4. Most common genres (+ share %, window over total)
g = genres.groupBy('genre').agg(countDistinct('show_id').alias('show_count'))
total_shows = g.agg(sum('show_count').alias('total')).collect()[0]['total']

gold_top_genres = (g.withColumn('pct_of_shows',
        round(100 * col('show_count') / total_shows, 1))
    .orderBy(desc('show_count')))

for name, df in [('gold_episodes_per_season', gold_eps_per_season),
                 ('gold_avg_runtime_per_show', gold_avg_runtime),
                 ('gold_top_cast', gold_top_cast),
                 ('gold_top_genres', gold_top_genres)]:
    
    df.write.format('delta').mode('overwrite') \
      .option('overwriteSchema','true').saveAsTable(f'{gold_schema_name}.{name}')
