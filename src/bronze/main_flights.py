# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests>=2.32.5",
# ]
# ///

from requester.LHRequester import LHRequester

import time
import os

def is_databricks_runtime() -> bool:
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

def is_databricks_job() -> bool:
    return os.getenv("DATABRICKS_JOB_ID") is not None

from utils.save import save_local, save_flights_databricks
from create_bronze_tables import create_bronze_tables

save_function = save_local
spark = None
if is_databricks_runtime():
    # In Databricks jobs/notebooks use the in-cluster SparkSession, not Databricks Connect.
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    create_bronze_tables(spark)

    save_function = save_flights_databricks

# Start requesting data
requester = LHRequester(save_function, spark)

start_time = time.perf_counter()

requester.fetch_flights()
print()

end_time = time.perf_counter()
elapsed_s = (end_time - start_time)
print(f"Finished fetching flights data in {elapsed_s:.2f} seconds")
