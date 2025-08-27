package com.flight

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Visualizer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Delay Visualizer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load CSV files
    val flightDF = spark.read.option("header", "true").option("inferSchema", "true").csv("data/fact_flight_delays.csv")
    val airlineDF = spark.read.option("header", "true").option("inferSchema", "true").csv("data/dim_airline.csv")
    val airportDF = spark.read.option("header", "true").option("inferSchema", "true").csv("data/dim_airport.csv")
    val weatherDF = spark.read.option("header", "true").option("inferSchema", "true").csv("data/dim_weather.csv")

    // Rename columns to avoid conflicts
    val originAirportDF = airportDF
      .withColumnRenamed("airport_id", "origin_airport_id")
      .withColumnRenamed("name", "origin_airport_name")
      .withColumnRenamed("city", "origin_city")

    val destAirportDF = airportDF
      .withColumnRenamed("airport_id", "dest_airport_id")
      .withColumnRenamed("name", "dest_airport_name")
      .withColumnRenamed("city", "dest_city")

    // Join with dimension tables
    val enrichedDF = flightDF
      .join(airlineDF, "airline_id")
      .join(originAirportDF, "origin_airport_id")
      .join(destAirportDF, "dest_airport_id")
      .join(weatherDF, "weather_id")

    // Add prediction columns
    val predictedDF = enrichedDF
      .withColumn("predicted_category", $"delay_category")
      .withColumn("is_correct", $"delay_minutes" > 0)

    // 📊 Distribution of Predicted Categories
    println("📊 Distribution of Predicted Categories:")
    predictedDF.groupBy("predicted_category").count().show()

    // ⏱️ Average Delay Minutes by Predicted Category
    println("⏱️ Average Delay Minutes by Predicted Category:")
    predictedDF.groupBy("predicted_category")
      .agg(avg($"delay_minutes").alias("avg_delay"))
      .show()

    // ✅ Prediction Accuracy
    val correctCount = predictedDF.filter($"is_correct" === true).count()
    val totalCount = predictedDF.count()
    val accuracy = if (totalCount > 0) correctCount.toDouble / totalCount else 0.0
    println(f"✅ Prediction Accuracy: $accuracy%.2f")

    // ✈️ Average Delay by Airline
    println("✈️ Average Delay by Airline:")
    predictedDF.groupBy("name") // airline name
      .agg(avg($"delay_minutes").alias("avg_delay"))
      .show()

    // 🏙️ Average Delay by Origin City
    println("🏙️ Average Delay by Origin City:")
    predictedDF.groupBy("origin_city")
      .agg(avg($"delay_minutes").alias("avg_delay"))
      .show()

    // 🌦️ Weather Impact on Delays
    println("🌦️ Weather Impact on Delays:")
    predictedDF.groupBy("temperature", "wind_speed", "visibility")
      .agg(avg($"delay_minutes").alias("avg_delay"))
      .orderBy(desc("avg_delay"))
      .show()

    spark.stop()
  }
}