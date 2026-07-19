# Databricks notebook source
# MAGIC %run ./nb_common_function

# COMMAND ----------

# MAGIC %md 
# MAGIC ## window functions

# COMMAND ----------

#-- Latest 3 episodes per show +  season running runtime + gap since previous episode

display(
    spark.sql(f'''
        WITH ep AS (
          SELECT show_id, season, episode_number, episode_name, air_date, runtime,
                 ROW_NUMBER()  OVER (PARTITION BY show_id ORDER BY air_date DESC) AS rn,
                 SUM(runtime)  OVER (PARTITION BY show_id, season
                                     ORDER BY episode_number
                                     ROWS UNBOUNDED PRECEDING)        AS season_running_runtime,
                 DATEDIFF(air_date,
                          LAG(air_date) OVER (PARTITION BY show_id ORDER BY air_date))
                                                                  AS days_since_prev
          FROM {silver_schema_name}.silver_episodes
        )
        SELECT *
        FROM ep
        WHERE rn <= 3
        ORDER BY show_id, rn
    ''')
)

# COMMAND ----------

# 2 - joins + GROUP BY/HAVING-- Genre performance: shows, episodes, avg runtime and rating per genre
display(
    spark.sql(
        f"""SELECT g.genre,
       COUNT(DISTINCT s.show_id)              AS shows,
       COUNT(e.episode_id)                    AS episodes,
       ROUND(AVG(e.runtime), 1)               AS avg_runtime,
       ROUND(AVG(s.rating),  2)               AS avg_show_rating
FROM silver.silver_shows s
JOIN silver.silver_show_genres g ON g.show_id = s.show_id
LEFT JOIN silver.silver_episodes e ON e.show_id = s.show_id
GROUP BY g.genre
HAVING COUNT(DISTINCT s.show_id)"""
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BEFORE (slow): function on join/filter column kills pruning; correlated subquery
# MAGIC SELECT s.show_name, e.episode_name, e.runtime
# MAGIC FROM silver.silver_episodes e
# MAGIC JOIN silver.silver_shows s ON CAST(e.show_id AS STRING) = CAST(s.show_id AS STRING)
# MAGIC WHERE YEAR(e.air_date) = 2024
# MAGIC   AND e.runtime > (SELECT AVG(e2.runtime) FROM silver.silver_episodes e2
# MAGIC                    WHERE e2.show_id = e.show_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC --AFTER (tuned): sargable predicate, native-type join, CTE replaces correlation
# MAGIC WITH show_avg AS (
# MAGIC   SELECT show_id, AVG(runtime) AS avg_rt
# MAGIC   FROM silver.silver_episodes GROUP BY show_id
# MAGIC )
# MAGIC SELECT s.show_name, e.episode_name, e.runtime
# MAGIC FROM silver.silver_episodes e
# MAGIC JOIN show_avg a ON a.show_id = e.show_id
# MAGIC JOIN silver.silver_shows s ON s.show_id = e.show_id
# MAGIC WHERE e.air_date >= '2024-01-01' AND e.air_date < '2025-01-01'
# MAGIC   AND e.runtime > a.avg_rt;

# COMMAND ----------

# MAGIC %md
# MAGIC #Performamce 
# MAGIC ### Indexes
# MAGIC - non-cluster index on episoden(air_date) INCLUDE (show_id, runtime,episode_name) covers the filter+join.
# MAGIC - index on shows(show_id)
# MAGIC - ZORDER BY (show_id) + partition on air_date month
# MAGIC
# MAGIC ### filter early 
# MAGIC - airdate >= '2024-01-01' is sargable: the optimizer pushes it to the storage scan (index seek /Delta file pruning) 
# MAGIC - YEAR(airdate)=2024 wraps the column in a function, forcing a full scan of
# MAGIC every row before filtering.
# MAGIC
# MAGIC ### CTEs / materialized views help
# MAGIC - If reused often then persist as a materialized view, so then aggregate is pre-computed. 
# MAGIC - In a correlated subquery, the database has to compute the inner query (AVG(runtime)) once for every outer row. That means the work scales as 𝑂(𝑛*𝑚)
# MAGIC - If you have 10,000 episodes (n) and each show has 100 episodes (m), that’s about 1,000,000 operations.
# MAGIC - In the CTE approach, the average per show is computed once (m), and then joined to all episodes (n). That scales as 𝑂(𝑛+𝑚), which is far more efficient. 
# MAGIC
# MAGIC
