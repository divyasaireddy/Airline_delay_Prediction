package com.flight

import org.apache.spark.sql.{SparkSession, DataFrame}

object DataLoader {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Delay Prediction")
      .master("local[*]")
      .getOrCreate()

    // Load CSV files
    val flightDF = loadCSV(spark, "data/fact_flight_delays.csv")
    val airlineDF = loadCSV(spark, "data/dim_airline.csv")
    val airportDF = loadCSV(spark, "data/dim_airport.csv")
    val weatherDF = loadCSV(spark, "data/dim_weather.csv")

    // Show sample data
    flightDF.show()
    airlineDF.show()
    airportDF.show()
    weatherDF.show()

    spark.stop()
  }

  def loadCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }
}