from pyspark.sql.functions import col, from_json, when, explode, max as max_col, lit, to_timestamp
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, ArrayType

def silverize_flights(spark):
	bronze_table = spark.table("lufthansa.bronze.flights")

	latest_ingestion_date = bronze_table.select(max_col("ingestion_date").alias("d")).first()["d"]
	if latest_ingestion_date is None:
		print("No data in lufthansa.bronze.flights, skipping silver load.")
		return

	# latest_bronze = bronze_table.filter(col("ingestion_date") == lit(latest_ingestion_date))

	bronze_schema = StructType([
		StructField("FlightStatusResource", StructType([
			StructField("Flights", StructType([
				StructField("Flight", ArrayType(
					StructType([
						StructField("Departure", StructType([
							StructField("AirportCode", StringType()),
							StructField("ScheduledTimeLocal", StructType([
								StructField("DateTime", StringType())
							])),
							StructField("ScheduledTimeUTC", StructType([
								StructField("DateTime", StringType())
							])),
							StructField("ActualTimeLocal", StructType([
								StructField("DateTime", StringType())
							])),
							StructField("ActualTimeUTC", StructType([
								StructField("DateTime", StringType())
							])),
							StructField("TimeStatus", StructType([
								StructField("Code", StringType()),
								StructField("Definition", StringType())
							])),
							StructField("Terminal", StructType([
								StructField("Name", StringType()),
								StructField("Gate", StringType())
							])),
						])),
						StructField("Arrival", StructType([
							StructField("AirportCode", StringType()),
							StructField("ScheduledTimeLocal", StructType([
								StructField("DateTime", StringType())
							])),
							StructField("ScheduledTimeUTC", StructType([
								StructField("DateTime", StringType())
							])),
							StructField("ActualTimeLocal", StructType([
								StructField("DateTime", StringType())
							])),
							StructField("ActualTimeUTC", StructType([
								StructField("DateTime", StringType())
							])),
							StructField("TimeStatus", StructType([
								StructField("Code", StringType()),
								StructField("Definition", StringType())
							])),
							StructField("Terminal", StructType([
								StructField("Name", StringType()),
								StructField("Gate", StringType())
							])),
						])),
						StructField("MarketingCarrier", StructType([
							StructField("AirlineID", StringType()),
							StructField("FlightNumber", StringType())
						])),
						StructField("OperatingCarrier", StructType([
							StructField("AirlineID", StringType()),
							StructField("FlightNumber", StringType())
						])),
						StructField("Equipment", StructType([
							StructField("AircraftCode", StringType()),
							StructField("AircraftRegistration", StringType())
						])),
						StructField("FlightStatus", StructType([
							StructField("Code", StringType()),
							StructField("Definition", StringType())
						])),
						StructField("ServiceType", StringType())
					])
				))
			]))
		]))
	])

	parsed_bronze_table =	bronze_table.select(
		from_json(col("raw_data"), bronze_schema).alias("parsed"),
		col("ingestion_date"),
		col("data_date").alias("flight_date"),
		col("data_time_block"),
		col("airport_code"),
		col("offset"),
	)

	parsed_flights = parsed_bronze_table.select(
		explode(col("parsed.FlightStatusResource.Flights.Flight")).alias("flight")
	)
	
	silver_table = (
		# Change this select to (Sahan said that this shouldn't be)
		# Steaming Table
		parsed_flights.select(
			col("flight.Departure.AirportCode").alias("departure_airport_code"),
			to_timestamp(col("flight.Departure.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm").alias("departure_scheduled_time_local"),
			to_timestamp(col("flight.Departure.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'").alias("departure_scheduled_time_utc"),
			to_timestamp(col("flight.Departure.ActualTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm").alias("departure_actual_time_local"),
			to_timestamp(col("flight.Departure.ActualTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'").alias("departure_actual_time_utc"),
			col("flight.Departure.TimeStatus.Code").alias("departure_time_status_code"),
			col("flight.Departure.TimeStatus.Definition").alias("departure_time_status_definition"),
			col("flight.Departure.Terminal.Name").alias("departure_terminal_name"),
			col("flight.Departure.Terminal.Gate").alias("departure_terminal_gate"),

			col("flight.Arrival.AirportCode").alias("arrival_airport_code"),
			to_timestamp(col("flight.Arrival.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm").alias("arrival_scheduled_time_local"),
			to_timestamp(col("flight.Arrival.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'").alias("arrival_scheduled_time_utc"),
			to_timestamp(col("flight.Arrival.ActualTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm").alias("arrival_actual_time_local"),
			to_timestamp(col("flight.Arrival.ActualTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'").alias("arrival_actual_time_utc"),
			col("flight.Arrival.TimeStatus.Code").alias("arrival_time_status_code"),
			col("flight.Arrival.TimeStatus.Definition").alias("arrival_time_status_definition"),
			col("flight.Arrival.Terminal.Name").alias("arrival_terminal_name"),
			col("flight.Arrival.Terminal.Gate").alias("arrival_terminal_gate"),

			col("flight.MarketingCarrier.AirlineID").alias("marketing_carrier_airline_id"),
			col("flight.MarketingCarrier.FlightNumber").alias("marketing_carrier_flight_number"),
			col("flight.OperatingCarrier.AirlineID").alias("operating_carrier_airline_id"),
			col("flight.OperatingCarrier.FlightNumber").alias("operating_carrier_flight_number"),
			col("flight.Equipment.AircraftCode").alias("equipment_aircraft_code"),
			col("flight.Equipment.AircraftRegistration").alias("equipment_aircraft_registration"),
			col("flight.FlightStatus.Code").alias("flight_status_code"),
			col("flight.FlightStatus.Definition").alias("flight_status_definition"),
			col("flight.ServiceType").alias("service_type"),
		)
	)

	silver_table.write.mode("overwrite").saveAsTable("lufthansa.silver.flights")

# 
