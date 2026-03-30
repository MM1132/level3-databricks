def create_silver_flights_table(spark):
	spark.sql("CREATE CATALOG IF NOT EXISTS lufthansa;")
	spark.sql("CREATE SCHEMA IF NOT EXISTS lufthansa.silver;")

	spark.sql(
		"""
		CREATE TABLE IF NOT EXISTS lufthansa.silver.flights (
		departure_airport_code STRING,
		departure_scheduled_time_local TIMESTAMP,
		departure_scheduled_time_utc TIMESTAMP,
		departure_actual_time_local TIMESTAMP,
		departure_actual_time_utc TIMESTAMP,
		departure_time_status_code STRING,
		departure_time_status_definition STRING,
		departure_terminal_name STRING,
		departure_terminal_gate STRING,

		arrival_airport_code STRING,
		arrival_scheduled_time_local TIMESTAMP,
		arrival_scheduled_time_utc TIMESTAMP,
		arrival_actual_time_local TIMESTAMP,
		arrival_actual_time_utc TIMESTAMP,
		arrival_time_status_code STRING,
		arrival_time_status_definition STRING,
		arrival_terminal_name STRING,
		arrival_terminal_gate STRING,
		
		marketing_carrier_airline_id STRING,
		marketing_carrier_flight_number STRING,
		operating_carrier_airline_id STRING,
		operating_carrier_flight_number STRING,
		equipment_aircraft_code STRING,
		equipment_aircraft_registration STRING,
		flight_status_code STRING,
		flight_status_definition STRING,
		service_type STRING
		)
		USING DELTA
	"""
	)
