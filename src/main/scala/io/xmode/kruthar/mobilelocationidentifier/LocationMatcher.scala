package io.xmode.kruthar.mobilelocationidentifier

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object LocationMatcher {
  val getUnitPointsUDF: UserDefinedFunction = udf(
    (lat: Double, lon: Double, d: Double) => {
      Geo.getUnitPoints(lat, lon, d)
    }
  )

  val distanceBetweenUDF: UserDefinedFunction = udf(
    (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      Geo.distanceBetween(lat1, lon1, lat2, lon2)
    }
  )

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

    val locationMatches = matchLocations(mobileData, locationData)

    if (config.outputPath.isEmpty) {
      println("Location Match Output Sample:")
      locationMatches.take(20).foreach(println)
    } else {
      locationMatches.write
        .format(config.outputFormat.getOrElse("csv"))
        .mode("overwrite")
        .save(config.outputPath.get)
    }
  }

  def matchLocations(mobileData: DataFrame, locationData: DataFrame): DataFrame = {
    // Create an expanded data set where each row is a unit lat/lon for one of the original location points.
    // Original location points will likely expand into multiple unit lat/lon rows here.
    val unitLocations = locationData
      .withColumn("unitArray", getUnitPointsUDF(col("Latitude"), col("Longitude"), col("Radius")))
      .withColumn("unitLocation", explode(col("unitArray")))
      .select(
        col("City"),
        col("Latitude"),
        col("Longitude"),
        col("Radius"),
        col("unitLocation"),
        element_at(col("unitLocation"), 1) as "unitLat",
        element_at(col("unitLocation"), 2) as "unitLon"
      )

    // In the mobile data round the lat/lons down to get the unit lat/lon that the mobile point is in
    val unitMobile = mobileData
      .withColumn("unitLat", floor(col("latitude")))
      .withColumn("unitLon", floor(col("longitude")))

    // Now we can use specific join conditions to drive a more efficient join.
    val totalUnitJoin = unitMobile
      .join(unitLocations, Seq("unitLat", "unitLon"))

    // Since we were using imprecise unit lat/lons to identify "possible" matches,
    // We now have to double check our matches with a direct distance check
    totalUnitJoin
      .withColumn("distanceMeters", distanceBetweenUDF(unitMobile("latitude"), unitMobile("longitude"), unitLocations("Latitude"), unitLocations("Longitude")))
      .where(col("distanceMeters") < unitLocations("Radius"))
      .select(
        unitMobile("advertiser_id"),
        unitMobile("location_at"),
        unitMobile("latitude"),
        unitMobile("longitude"),
        unitLocations("City")
      )
  }
}

