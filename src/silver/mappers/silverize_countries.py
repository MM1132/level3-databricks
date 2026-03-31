from pyspark.sql.functions import col, from_json, when, explode, max as max_col, lit
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, ArrayType

def silverize_countries(spark):
	bronze_table = spark.table("lufthansa.bronze.countries")

	latest_ingestion_date = bronze_table.select(max_col("ingestion_date").alias("d")).first()["d"]
	if latest_ingestion_date is None:
		print("No data in lufthansa.bronze.countries, skipping silver load.")
		return

	latest_bronze = bronze_table.filter(col("ingestion_date") == lit(latest_ingestion_date))

	bronze_schema = StructType([
		StructField("CountryResource", StructType([
			StructField("Countries", StructType([
				StructField("Country", ArrayType(
					StructType([
						StructField("CountryCode", StringType()),
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

	parsed_countries = parsed_bronze_table.select(
		explode(col("parsed.CountryResource.Countries.Country")).alias("country")
	)

	silver_table = (
		parsed_countries.select(
			col("country.CountryCode").alias("country_code"),
			col("country.Names.Name.`$`").alias("name")
		)
		.filter(col("country_code").isNotNull())
		.dropDuplicates(["country_code"])
	)

	silver_table.write.mode("overwrite").saveAsTable("lufthansa.silver.countries")
