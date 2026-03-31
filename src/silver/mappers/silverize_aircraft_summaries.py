from pyspark.sql.functions import col, from_json, when, explode, max as max_col, lit
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, ArrayType

def silverize_aircraft_summaries(spark):
	bronze_table = spark.table("lufthansa.bronze.aircraft_summaries")

	latest_ingestion_date = bronze_table.select(max_col("ingestion_date").alias("d")).first()["d"]
	if latest_ingestion_date is None:
		print("No data in lufthansa.bronze.aircraft_summaries, skipping silver load.")
		return

	latest_bronze = bronze_table.filter(col("ingestion_date") == lit(latest_ingestion_date))

	bronze_schema = StructType([
		StructField("AircraftResource", StructType([
			StructField("AircraftSummaries", StructType([
				StructField("AircraftSummary", ArrayType(
					StructType([
						StructField("AircraftCode", StringType()),
						StructField("Names", StructType([
								StructField("Name", StructType([
										StructField("$", StringType())
								]))
						])),
						StructField("AirlineEquipCode", StringType())
					])
				))
			]))
		]))
	])

	parsed_bronze_table =	latest_bronze.select(
		from_json(col("raw_data"), bronze_schema).alias("parsed"),
		col("ingestion_date")
	)

	parsed_aircraft_summaries = parsed_bronze_table.select(
		explode(col("parsed.AircraftResource.AircraftSummaries.AircraftSummary")).alias("aircraft_summary")
	)

	silver_table = (
		parsed_aircraft_summaries.select(
			col("aircraft_summary.AircraftCode").alias("aircraft_code"),
			col("aircraft_summary.Names.Name.`$`").alias("name"),
			col("aircraft_summary.AirlineEquipCode").alias("airline_equip_code")
		)
		.filter(col("aircraft_code").isNotNull())
		.dropDuplicates(["aircraft_code"])
	)

	silver_table.write.mode("overwrite").saveAsTable("lufthansa.silver.aircraft_summaries")
