import pytest
from pyspark.sql import SparkSession
from src/transformations import clean_episodes

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('dq-tests').getOrCreate()

def _sample(spark):
    rows = [
        {'id': 1, 'show_id': 10, 'name': ' Pilot ', 'season': 1,
         'airdate': '2024-01-05', 'runtime': 45},
        {'id': 1, 'show_id': 10, 'name': 'Pilot dup', 'season': 1,
         'airdate': '2024-01-05', 'runtime': 45},          # duplicate id
        {'id': 2, 'show_id': 10, 'name': 'Ep2', 'season': 1,
         'airdate': '2024-01-12', 'runtime': -5},           # invalid runtime
    ]
    return spark.createDataFrame(rows)
def test_required_fields_not_null(spark):
    out = clean_episodes(_sample(spark))
    assert out.filter('episode_id IS NULL OR show_id IS NULL').count() == 0
def test_runtime_positive_or_null(spark):
    out = clean_episodes(_sample(spark))
    assert out.filter('runtime <= 0').count() == 0
def test_ids_unique(spark):
    out = clean_episodes(_sample(spark))
    assert out.count() == out.select('episode_id').distinct().count()
