from create_silver_reference_tables import create_silver_reference_tables
from mappers.silverize_airports import silverize_airports
from mappers.silverize_aircraft_summaries import silverize_aircraft_summaries
from mappers.silverize_airlines import silverize_airlines
from mappers.silverize_cities import silverize_cities
from mappers.silverize_countries import silverize_countries

from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

create_silver_reference_tables(spark)

silverize_airports(spark)
silverize_aircraft_summaries(spark)
silverize_airlines(spark)
silverize_cities(spark)
silverize_countries(spark)
