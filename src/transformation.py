from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when, to_date


def clean_episodes(df: DataFrame) -> DataFrame:
    return (
        df.select(
            col('id').cast('int').alias('episode_id'),
            col('show_id').cast('int'),
            trim(col('name')).alias('episode_name'),
            col('season').cast('int'),
            to_date('airdate').alias('airdate'),
            col('runtime').cast('int'),
        )
        .withColumn('runtime', when(col('runtime') > 0, col('runtime')))
        .dropDuplicates(['episode_id'])
    )
