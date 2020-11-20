package io.xmode.kruthar.mobilelocationidentifier

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LocationMatcher {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate
    val config = ArgParser.parse(args)

    // Ideally, this would pull from a persisted big data source using a metastore, or with S3 support etc.
    // With a better organized data source, date filtering would likely be done by physical partition instead of by the location_at.
    // But, for the purposes of this assignment we will just use local file paths.
    val mobileData = spark.read
      .format("parquet")
      .load(config.mobilePath)
      .filter(col("location_at").between(lit(config.start), lit(config.end)))

    val locationData = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(config.locationPath)

    println("mobile data")
    mobileData.take(10).foreach(println)

    println("location data")
    locationData.take(10).foreach(println)
  }
}

