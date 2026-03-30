from pyspark.sql.functions import (
  col,
  to_date,
  lit,
  count,
  avg,
  when,
  unix_timestamp,
  round,
  coalesce,
)


def goldenize_airports_daily_stats(spark):
  gold_table_name = "lufthansa.gold.airports_daily_stats"

  silver_flights_table = spark.table("lufthansa.silver.flights")
  silver_airports_table = spark.table("lufthansa.silver.airports")

  flights_with_date = silver_flights_table.select(
    col("departure_airport_code"),
    to_date(col("departure_scheduled_time_local")).alias("date"),
    (
      (unix_timestamp(col("departure_actual_time_utc")) - unix_timestamp(col("departure_scheduled_time_utc")))
      / lit(60.0)
    ).alias("departure_delay_minutes"),
  ).where(
    col("departure_airport_code").isNotNull() & col("date").isNotNull()
  )

  if flights_with_date.limit(1).count() == 0:
    print("No valid data in lufthansa.silver.flights, skipping gold load.")
    return

  yesterday_date = spark.sql("SELECT date_sub(current_date(), 1) AS d").first()["d"]
  day_before_yesterday_date = spark.sql("SELECT date_sub(current_date(), 2) AS d").first()["d"]

  if spark.catalog.tableExists(gold_table_name):
    existing_gold_dates = spark.table(gold_table_name).select("date").distinct()
  else:
    existing_gold_dates = spark.createDataFrame([], "date DATE")

  older_candidate_dates = (
    flights_with_date
    .filter(col("date") <= lit(day_before_yesterday_date))
    .select("date")
    .distinct()
  )

  older_dates_to_process = older_candidate_dates.join(existing_gold_dates, ["date"], "left_anti")
  has_older_dates_to_process = older_dates_to_process.limit(1).count() > 0
  has_yesterday_data = flights_with_date.filter(col("date") == lit(yesterday_date)).limit(1).count() > 0

  if not has_older_dates_to_process and not has_yesterday_data:
    print("No new historical dates and no yesterday data to refresh, skipping gold load.")
    return

  dates_to_process = older_dates_to_process
  if has_yesterday_data:
    yesterday_date_df = spark.createDataFrame([(yesterday_date,)], ["date"])
    dates_to_process = dates_to_process.unionByName(yesterday_date_df).dropDuplicates(["date"])

  flights_to_process = flights_with_date.join(dates_to_process, ["date"], "inner")

  aggregated_flights = flights_to_process.groupBy(
    col("departure_airport_code").alias("airport_code"),
    col("date"),
  ).agg(
    count(lit(1)).cast("int").alias("total_departures"),
    round(avg(col("departure_delay_minutes")), 2).alias("average_departure_delay_minutes"),
    count(when(col("departure_delay_minutes") > lit(15), lit(1))).cast("int").alias("delayed_flights"),
  )

  airport_dates = (
    silver_airports_table
    .select("airport_code", "longitude", "latitude")
    .crossJoin(dates_to_process)
  )

  gold_rows = (
    airport_dates.alias("a")
    .join(
      aggregated_flights.alias("f"),
      (
        (col("a.airport_code") == col("f.airport_code"))
        & (col("a.date") == col("f.date"))
      ),
      "left",
    )
    .select(
      col("a.airport_code").alias("airport_code"),
      col("a.longitude").alias("longitude"),
      col("a.latitude").alias("latitude"),
      coalesce(col("f.total_departures"), lit(0)).cast("int").alias("total_departures"),
      col("f.average_departure_delay_minutes").alias("average_departure_delay_minutes"),
      coalesce(col("f.delayed_flights"), lit(0)).cast("int").alias("delayed_flights"),
      when(
        coalesce(col("f.total_departures"), lit(0)) > lit(0),
        round((col("f.delayed_flights") / col("f.total_departures")) * lit(100.0), 2),
      )
      .otherwise(lit(0.0))
      .alias("delayed_flights_percentage"),
      col("a.date").alias("date"),
    )
  )

  if has_older_dates_to_process:
    older_gold_rows = gold_rows.join(older_dates_to_process, ["date"], "inner")
    older_gold_rows.write.mode("append").saveAsTable(gold_table_name)

  if has_yesterday_data:
    yesterday_gold_rows = gold_rows.filter(col("date") == lit(yesterday_date))
    yesterday_str = yesterday_date.isoformat()
    (
      yesterday_gold_rows.write
      .format("delta")
      .mode("overwrite")
      .option("replaceWhere", f"date = DATE '{yesterday_str}'")
      .saveAsTable(gold_table_name)
    )

  print("Finished goldenizing airports daily stats for incremental historical dates and yesterday refresh.")
