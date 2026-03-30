from create_gold_tables import create_gold_tables
from goldenize_airports_daily_stats import goldenize_airports_daily_stats

from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

create_gold_tables(spark)
goldenize_airports_daily_stats(spark)
