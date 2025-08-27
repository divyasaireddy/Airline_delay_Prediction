package com.flight

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object ModelTrainer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Delay Rule-Based Predictor")
      .master("local[*]")
      .getOrCreate()

    // Load CSV files
    val flightDF = spark.read.option("header", "true").csv("data/fact_flight_delays.csv")
    val airlineDF = spark.read.option("header", "true").csv("data/dim_airline.csv")
    val airportDF = spark.read.option("header", "true").csv("data/dim_airport.csv")
    val weatherDF = spark.read.option("header", "true").csv("data/dim_weather.csv")

    // Join and enrich
    val enrichedDF = flightDF
      .join(airlineDF, "airline_id")
      .join(weatherDF, "weather_id")
      .join(airportDF, "airport_id")
      .withColumn("scheduled_hour", hour(to_timestamp(col("scheduled_departure"))))
      .withColumn("scheduled_day", dayofweek(to_timestamp(col("scheduled_departure"))))

    // Apply rule-based classification
    val predictedDF = enrichedDF.withColumn("predicted_category",
      when(col("delay_minutes").cast("int") > 30, "Long")
        .when(col("delay_minutes").cast("int") > 0, "Short")
        .otherwise("On-time")
    )

    // Evaluate accuracy
    val evaluatedDF = predictedDF.withColumn("is_correct",
      col("predicted_category") === col("delay_category")
    )

    val correctCount = evaluatedDF.filter(col("is_correct")).count()
    val totalCount = evaluatedDF.count()
    val accuracy = if (totalCount > 0) correctCount.toDouble / totalCount else 0.0
    println(s"Rule-Based Prediction Accuracy: $accuracy")

    // Show predictions
    predictedDF.select("flight_id", "delay_minutes", "delay_category", "predicted_category").show()

    // Optional: Save predictions
    predictedDF.write.option("header", "true").mode("overwrite")
      .csv("C:/Users/sadivya/Downloads/FlightDelayPrediction/output/predictions")
    println("✅ Predictions saved to: C:/Users/sadivya/Downloads/FlightDelayPrediction/output/predictions")
    spark.stop()
  }
}