package io.xmode.kruthar.mobilelocationidentifier

import math._

object Geo {
  // Radius of Earth in meters
  lazy val radius = 6371000

  /**
   * This function takes a lat/lon point and a distance.
   * It produces an array of unit lat/long points for which the lat/lon square
   * contains area within the distance d from the given point.
   *
   * @param lat - latitude of point in degrees
   * @param lon - longitude of point in degrees
   * @param d - distance from point in meters
   * @return - an Array of unit lat/lon's that intersect the circle of radius d with center point
   */
  def getUnitPoints(lat: Double, lon: Double, d: Double): Array[Array[Int]] = {
    // Round lat/longs to the left unit square
    val latU = (if (lat < 1) ceil(lat) else floor(lat)).toInt
    val lonU = (if (lon < 1) ceil(lon) else floor(lon)).toInt

    // Get distance between lat/lons respectively at this point
    // Roughly average distance between latitude lines.
    // NOTE: This is also simplified and can be made more accurate
    // https://en.wikipedia.org/wiki/Latitude
    val latD = 111000
    // Given a latitude in degrees, return the distance between longitude lines at that point
    // NOTE: This is the simplest formula for this, based on spherical Earth. This could be accuracy improved
    // https://en.wikipedia.org/wiki/Longitude
    val lonD = if (abs(lat) >= 90) 0 else (Pi / 180) * radius * cos(lat * Pi / 180)

    // Get the number of lat/lon lines respectively that can be reached with distance d
    val latN = ceil(d / latD).toInt
    val lonN = ceil(d / lonD).toInt

    // Now find all unit lat/lons that contain area within the distance d of our point
    // The Ranges specified are inclusive so we cover the fact that we rounded to the left our lat/lon to begin with
    // THIS IS A SCARY M*N, but it really isn't that bad. For most instances this will not produce more then a handful of unit lat/lons.
    // This array gets bigger as the distance increases (order of kilometers) or the closer you are to polls (because longitude distance is less).
    // But even near the polls (85, 35) and a distance of 50km, it only produces 39 unit lat/lons.
    (-latN to latN).flatMap(lat => (-lonN to lonN).map(lon => Array(lat + latU, lon + lonU))).toArray
  }

  // Give the distance (in meters) between the given lat,long coordinate pairs in degrees.
  // https://www.movable-type.co.uk/scripts/latlong.html
  /**
   * Given two lat/lon points, provide the "great circle" distance in meters
   *
   * @param latitude1 - lat for point 1
   * @param longitude1 - lon for point 1
   * @param latitude2 - lat for point 2
   * @param longitude2 - lon for point 2
   * @return - the distance between points (in meters)
   */
  def distanceBetween(latitude1: Double, longitude1: Double, latitude2: Double, longitude2: Double): Double = {
    val lat1 = degToRad(latitude1)
    val long1 = degToRad(longitude1)
    val lat2 = degToRad(latitude2)
    val long2 = degToRad(longitude2)
    val diffLat = lat1 - lat2
    val diffLong = long1 - long2

    val a = pow(sin(diffLat / 2), 2) + cos(lat1) * cos(lat2) * pow(sin(diffLong / 2), 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    val d = radius * c

    d
  }

  /**
   * Convert degrees to radians
   * @param deg - degrees to convert
   * @return radians
   */
  def degToRad(deg: Double): Double = deg * Pi / 180
}
