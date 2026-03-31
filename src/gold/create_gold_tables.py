def create_gold_tables(spark):
	spark.sql("CREATE CATALOG IF NOT EXISTS lufthansa;")
	spark.sql("CREATE SCHEMA IF NOT EXISTS lufthansa.gold;")

	spark.sql("""
		CREATE TABLE IF NOT EXISTS lufthansa.gold.airports_daily_stats (
			airport_code STRING,
			longitude DOUBLE,
			latitude DOUBLE,
			total_departures INT,
			average_departure_delay_minutes DOUBLE,
			delayed_flights INT,
			delayed_flights_percentage DOUBLE,
			date DATE,
			hour INT
		)
		USING DELTA
	""")
