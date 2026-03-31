from pyspark.sql.functions import col, from_json, when, explode, max as max_col, lit
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, ArrayType

"""
Silver table structure:

CREATE TABLE IF NOT EXISTS lufthansa.silver.cities (
    city_code STRING,
    country_code STRING,
    name STRING,
    utc_offset DOUBLE,
    time_zone_id STRING,
    airport_codes ARRAY<STRING>
)
USING DELTA
"""

def silverize_cities(spark):
	bronze_table = spark.table("lufthansa.bronze.cities")

	latest_ingestion_date = bronze_table.select(max_col("ingestion_date").alias("d")).first()["d"]
	if latest_ingestion_date is None:
		print("No data in lufthansa.bronze.cities, skipping silver load.")
		return

	latest_bronze = bronze_table.filter(col("ingestion_date") == lit(latest_ingestion_date))

	bronze_schema = StructType([
		StructField("CityResource", StructType([
			StructField("Cities", StructType([
				StructField("City", ArrayType(
					StructType([
						StructField("CityCode", StringType()),
						StructField("CountryCode", StringType()),
						StructField("Names", StructType([
                            StructField("Name", StructType([
                                    StructField("$", StringType())
                            ]))
						])),
						StructField("UtcOffset", StringType()),
						StructField("TimeZoneId", StringType()),
					])
				))
			]))
		]))
	])

	parsed_bronze_table =	latest_bronze.select(
		from_json(col("raw_data"), bronze_schema).alias("parsed"),
		col("ingestion_date")
	)

	parsed_cities = parsed_bronze_table.select(
		explode(col("parsed.CityResource.Cities.City")).alias("city")
	)

	silver_table = (
		parsed_cities.select(
			col("city.CityCode").alias("city_code"),
			col("city.CountryCode").alias("country_code"),
			col("city.Names.Name.`$`").alias("name"),
			when(col("city.UtcOffset").isNotNull(),
				(
					when(col("city.UtcOffset").startswith("+"), 1)
					.when(col("city.UtcOffset").startswith("-"), -1)
					.otherwise(0)
				) * (
					col("city.UtcOffset").substr(2, 2).cast(DoubleType()) +
					when(col("city.UtcOffset").substr(5, 2).isNotNull(),
						col("city.UtcOffset").substr(5, 2).cast(DoubleType()) / 60
					).otherwise(0)
				)
			).alias("utc_offset"),
			col("city.TimeZoneId").alias("time_zone_id"),
		)
		.filter(col("city_code").isNotNull())
		.dropDuplicates(["city_code"])
	)

	silver_table.write.mode("overwrite").saveAsTable("lufthansa.silver.cities")
