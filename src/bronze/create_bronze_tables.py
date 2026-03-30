def create_bronze_tables(spark):
    spark.sql("CREATE CATALOG IF NOT EXISTS lufthansa;")
    spark.sql("CREATE SCHEMA IF NOT EXISTS lufthansa.bronze;")

    bronze_reference_tables = [
        "airports",
        "aircraft_summaries",
        "airlines",
        "cities",
        "countries",
    ]

    for table_name in bronze_reference_tables:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS lufthansa.bronze.{table_name} (
                raw_data STRING,
                ingestion_date DATE,
                data_date DATE,
                offset INT
            )
            USING DELTA
        """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lufthansa.bronze.flights (
            raw_data STRING,
            ingestion_date DATE,
            data_date DATE,
            data_time_block STRING,
            airport_code STRING,
            offset INT
        )
        USING DELTA
    """)
