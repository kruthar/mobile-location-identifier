package io.xmode.kruthar.mobilelocationidentifier

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSpec

class LocationMatcherTest extends FunSpec {
  LogManager.getLogger("org").setLevel(Level.WARN) // Spark is very noisy

  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.sqlContext.implicits._

  def assertDataFramesEqual(left: DataFrame, right: DataFrame): Unit = {
    assert(left.collect.toSet === right.collect.toSet)
  }

  describe("LocationMatcher matchLocations Tests") {
    it("should properly identify points inside a location radius") {
      val points = Seq(
        Point("near", 0, 35.0004, 35.0002), // 48 meters away from 35,35
        Point("far ", 1, 35.0004, 35.0003) // 52 meters away from 35,35
      ).toDF
      val locations = Seq(
        Location("Rlyeh", 35, 35, 50)
      ).toDF
      val matches = Seq(
        Matched("near", 0, 35.0004, 35.0002, "Rlyeh")
      ).toDF

      assertDataFramesEqual(LocationMatcher.matchLocations(points, locations), matches)
    }

    it("should properly identify points inside multiple location radii") {
      val points = Seq(
        Point("near", 0, 35.0004, 35.0002), // 48 meters away from 35,35
        Point("far ", 1, 35.0004, 35.0003) // 52 meters away from 35,35
      ).toDF
      val locations = Seq(
        Location("Rlyeh", 35, 35, 50),
        Location("Leng", 35.0005, 35.0005, 50)
      ).toDF
      val matches = Seq(
        Matched("near", 0, 35.0004, 35.0002, "Rlyeh"),
        Matched("near", 0, 35.0004, 35.0002, "Leng"),
        Matched("far ", 1, 35.0004, 35.0003, "Leng")
      ).toDF

      assertDataFramesEqual(LocationMatcher.matchLocations(points, locations), matches)
    }
  }
}

case class Point (
                   advertiser_id: String,
                   location_at: Long,
                   latitude: Double,
                   longitude: Double
                 )

case class Location (
                      City: String,
                      Latitude: Double,
                      Longitude: Double,
                      Radius: Double
                    )

case class Matched (
                     advertiser_id: String,
                     location_at: Long,
                     latitude: Double,
                     longitude: Double,
                     City: String
                   )
