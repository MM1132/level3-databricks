from pyspark.sql.functions import col, from_json, when, explode, max as max_col, lit
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, ArrayType

def silverize_airports(spark):
	bronze_table = spark.table("lufthansa.bronze.airports")

	latest_ingestion_date = bronze_table.select(max_col("ingestion_date").alias("d")).first()["d"]
	if latest_ingestion_date is None:
		print("No data in lufthansa.bronze.airports, skipping silver load.")
		return

	latest_bronze = bronze_table.filter(col("ingestion_date") == lit(latest_ingestion_date))

	bronze_schema = StructType([
		StructField("AirportResource", StructType([
			StructField("Airports", StructType([
				StructField("Airport", ArrayType(
					StructType([
						StructField("AirportCode", StringType()),
						StructField("Position", StructType([
								StructField("Coordinate", StructType([
										StructField("Latitude",  DoubleType()),
										StructField("Longitude", DoubleType()),
								]))
						])),
						StructField("CityCode", StringType()),
						StructField("CountryCode", StringType()),
						StructField("LocationType", StringType()),
						StructField("Names", StructType([
								StructField("Name", StructType([
										StructField("$", StringType())
								]))
						])),
						StructField("UtcOffset", StringType()),
						StructField("TimeZoneId", StringType())
					])
				))
			]))
		]))
	])

	parsed_bronze_table =	latest_bronze.select(
		from_json(col("raw_data"), bronze_schema).alias("parsed"),
		col("ingestion_date")
	)

	parsed_airports = parsed_bronze_table.select(
		explode(col("parsed.AirportResource.Airports.Airport")).alias("airport")
	)

	silver_table = (
		parsed_airports.select(
			col("airport.AirportCode").alias("airport_code"),
			col("airport.Position.Coordinate.Longitude").alias("longitude").cast(DoubleType()),
			col("airport.Position.Coordinate.Latitude").alias("latitude").cast(DoubleType()),
			col("airport.CityCode").alias("city_code"),
			col("airport.CountryCode").alias("country_code"),
			col("airport.LocationType").alias("location_type"),
			col("airport.Names.Name.`$`").alias("name"),
			# A string such as "+01:00" or "-05:00", or even "+2:30"
			# Converted into double such as 1, -5, or even 2.5
			# airport.UtcOffset as utc_offset
			when(col("airport.UtcOffset").isNotNull(),
				(
					when(col("airport.UtcOffset").startswith("+"), 1)
					.when(col("airport.UtcOffset").startswith("-"), -1)
					.otherwise(0)
				) * (
					col("airport.UtcOffset").substr(2, 2).cast(DoubleType()) +
					when(col("airport.UtcOffset").substr(5, 2).isNotNull(),
						col("airport.UtcOffset").substr(5, 2).cast(DoubleType()) / 60
					).otherwise(0)
				)
			).alias("utc_offset"),
			col("airport.TimeZoneId").alias("time_zone_id")
		)
		.filter(col("airport_code").isNotNull())
		.filter(col("location_type") == "Airport")
		.dropDuplicates(["airport_code"])
	)

	silver_table.write.mode("overwrite").saveAsTable("lufthansa.silver.airports")
