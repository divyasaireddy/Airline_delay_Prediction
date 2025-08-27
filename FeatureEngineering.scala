package com.flight

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object FeatureEngineering {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Delay Feature Engineering")
      .master("local[*]")
      .getOrCreate()

    // Load CSVs
    val flightDF = DataLoader.loadCSV(spark, "data/fact_flight_delays.csv")
    val airlineDF = DataLoader.loadCSV(spark, "data/dim_airline.csv")
    val airportDF = DataLoader.loadCSV(spark, "data/dim_airport.csv")
    val weatherDF = DataLoader.loadCSV(spark, "data/dim_weather.csv")

    // Join tables
    val enrichedDF = flightDF
      .join(airlineDF, "airline_id")
      .join(weatherDF, "weather_id")

    // Feature: Extract hour and day from scheduled_departure
    val finalDF = enrichedDF
      .withColumn("scheduled_hour", hour(to_timestamp(col("scheduled_departure"))))
      .withColumn("scheduled_day", dayofweek(to_timestamp(col("scheduled_departure"))))

    finalDF.show()

    spark.stop()
  }
}