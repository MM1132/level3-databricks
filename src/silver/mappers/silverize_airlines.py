from pyspark.sql.functions import col, from_json, when, explode, max as max_col, lit
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, ArrayType

def silverize_airlines(spark):
	bronze_table = spark.table("lufthansa.bronze.airlines")

	latest_ingestion_date = bronze_table.select(max_col("ingestion_date").alias("d")).first()["d"]
	if latest_ingestion_date is None:
		print("No data in lufthansa.bronze.airlines, skipping silver load.")
		return

	latest_bronze = bronze_table.filter(col("ingestion_date") == lit(latest_ingestion_date))

	bronze_schema = StructType([
		StructField("AirlineResource", StructType([
			StructField("Airlines", StructType([
				StructField("Airline", ArrayType(
					StructType([
						StructField("AirlineID", StringType()),
						StructField("AirlineID_ICAO", StringType()),
						StructField("Names", StructType([
								StructField("Name", StructType([
										StructField("$", StringType())
								]))
						])),
					])
				))
			]))
		]))
	])

	parsed_bronze_table =	latest_bronze.select(
		from_json(col("raw_data"), bronze_schema).alias("parsed"),
		col("ingestion_date")
	)

	parsed_airlines = parsed_bronze_table.select(
		explode(col("parsed.AirlineResource.Airlines.Airline")).alias("airline")
	)

	silver_table = (
		parsed_airlines.select(
			col("airline.AirlineID").alias("airline_id"),
			col("airline.AirlineID_ICAO").alias("airline_id_icao"),
			col("airline.Names.Name.`$`").alias("name")
		)
		.filter(col("airline_id").isNotNull())
		.dropDuplicates(["airline_id"])
	)

	silver_table.write.mode("overwrite").saveAsTable("lufthansa.silver.airlines")
