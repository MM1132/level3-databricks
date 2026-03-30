from pathlib import Path
import json
from requester.request_config import RequestConfig
from .get_time import get_date_str_today

def save_local(spark, config: RequestConfig, offset: int, jsonData: str):
    date_today = get_date_str_today()
    save_dir = Path(__file__).resolve().parent.parent.parent.parent/"bronze_data"/config.container_key/date_today
    filename = f"{config.container_key}_{offset}.json"
    full_filename = save_dir/filename

    full_filename.parent.mkdir(parents=True, exist_ok=True)
    with open(full_filename, "w") as f:
        json.dump(jsonData, f)

def save_flights_local(spark, json_data: str, airport_from: str, offset: int, date_time_block: str):
    date = date_time_block.split("T")[0]
    hour_block = date_time_block.split("T")[1].split(":")[0]

    save_dir = Path(__file__).resolve().parent.parent.parent.parent/"bronze_data"/"Flights"/date/hour_block
    filename = f"Flights_from_{airport_from}_{date_time_block}_offset_{offset}.json"

    full_filename = save_dir/filename

    full_filename.parent.mkdir(parents=True, exist_ok=True)
    with open(full_filename, "w") as f:
        json.dump(json_data, f)

def save_databricks_reference_data(spark, config: RequestConfig, offset: int, jsonData: str):
    table_by_container_key = {
        "Airports": "lufthansa.bronze.airports",
        "AircraftSummaries": "lufthansa.bronze.aircraft_summaries",
        "Airlines": "lufthansa.bronze.airlines",
        "Cities": "lufthansa.bronze.cities",
        "Countries": "lufthansa.bronze.countries",
    }

    table_name = table_by_container_key.get(config.container_key)
    if table_name is None:
        raise ValueError(f"Unsupported reference container key: {config.container_key}")

    # Persist payload as a plain JSON string in raw_data.
    raw_json_str = json.dumps(jsonData)

    from pyspark.sql import functions as F

    row_df = (
        spark.createDataFrame([(raw_json_str, int(offset))], ["raw_data", "offset"])
            .withColumn("offset", F.col("offset").cast("int"))
            .withColumn("ingestion_date", F.current_date())
            .withColumn("data_date", F.current_date())
            .select("raw_data", "ingestion_date", "data_date", "offset")
    )

    row_df.write.mode("append").saveAsTable(table_name)

def save_flights_databricks(spark, json_data: str, airport_from: str, offset: int, date_time_block: str):
    flight_date = date_time_block.split("T")[0]
    block_start_hour = date_time_block.split("T")[1].split(":")[0]
    raw_json_str = json.dumps(json_data)

    from pyspark.sql import functions as F

    row_df = (
        spark.createDataFrame(
            [(raw_json_str, flight_date, block_start_hour, airport_from, int(offset))],
            ["raw_data", "data_date_raw", "data_time_block", "airport_code", "offset"],
        )
        .withColumn("offset", F.col("offset").cast("int"))
        .withColumn("ingestion_date", F.current_date())
        .withColumn("data_date", F.to_date("data_date_raw"))
        .select(
            "raw_data",
            "ingestion_date",
            "data_date",
            "data_time_block",
            "airport_code",
            "offset",
        )
    )

    row_df.write.mode("append").saveAsTable("lufthansa.bronze.flights")
