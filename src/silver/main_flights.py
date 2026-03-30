from create_silver_flights_table import create_silver_flights_table
from mappers.silverize_flights import silverize_flights

from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

create_silver_flights_table(spark)

silverize_flights(spark)
