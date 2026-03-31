def create_silver_reference_tables(spark):
	spark.sql("CREATE CATALOG IF NOT EXISTS lufthansa;")
	spark.sql("CREATE SCHEMA IF NOT EXISTS lufthansa.silver;")

	spark.sql("""
		CREATE TABLE IF NOT EXISTS lufthansa.silver.airports (
			airport_code STRING,
			longitude DOUBLE,
			latitude DOUBLE,
			city_code STRING,
			country_code STRING,
			location_type STRING,
			name STRING,
			utc_offset DOUBLE,
			time_zone_id STRING
		)
		USING DELTA
	""")

	spark.sql("""
		CREATE TABLE IF NOT EXISTS lufthansa.silver.aircraft_summaries (
			aircraft_code STRING,
			name STRING,
			airline_equip_code STRING
		)
		USING DELTA
	""")

	spark.sql("""
		CREATE TABLE IF NOT EXISTS lufthansa.silver.airlines (
			airline_id STRING,
			airline_id_icao STRING,
			name STRING
		)
		USING DELTA
	""")

	spark.sql("""
		CREATE TABLE IF NOT EXISTS lufthansa.silver.cities (
			city_code STRING,
			country_code STRING,
			name STRING,
			utc_offset DOUBLE,
			time_zone_id STRING
		)
		USING DELTA
	""")

	spark.sql("""
		CREATE TABLE IF NOT EXISTS lufthansa.silver.countries (
			country_code STRING,
			name STRING
		)
		USING DELTA
	""")
