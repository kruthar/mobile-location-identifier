package io.xmode.kruthar.mobilelocationidentifier

import org.scalatest.FunSpec

class GeoTest extends FunSpec {
  def assertLatLonArrayEqual(left: Array[Array[Int]], right: Array[Array[Int]]): Unit = {
    assert(left.map(_.mkString(",")).sorted.sameElements(right.map(_.mkString(",")).sorted))
  }
  
  describe("Geo getUnitPoints Tests") {
    it("should produce the minimum 9 squares with tiny radius") {
      val points = Geo.getUnitPoints(0, 0, 50)
      val expectedPoints = Array(
        Array(-1,  1), Array(0,  1), Array(1,  1),
        Array(-1,  0), Array(0,  0), Array(1,  0),
        Array(-1, -1), Array(0, -1), Array(1, -1)
      )

      assertLatLonArrayEqual(points, expectedPoints)
    }

    it("should produce proper squares with non unit lat/lon") {
      val points = Geo.getUnitPoints(45.587, 70.321, 100)
      val expectedPoints = Array(
        Array(44, 71), Array(45, 71), Array(46, 71),
        Array(44, 70), Array(45, 70), Array(46, 70),
        Array(44, 69), Array(45, 69), Array(46, 69)
      )

      assertLatLonArrayEqual(points, expectedPoints)
    }

    it("should produce proper squares with negative lat") {
      val points = Geo.getUnitPoints(-45.587, 70.321, 100)
      val expectedPoints = Array(
        Array(-44, 71), Array(-45, 71), Array(-46, 71),
        Array(-44, 70), Array(-45, 70), Array(-46, 70),
        Array(-44, 69), Array(-45, 69), Array(-46, 69)
      )

      assertLatLonArrayEqual(points, expectedPoints)
    }

    it("should produce proper squares with negative lon") {
      val points = Geo.getUnitPoints(45.587, -70.321, 100)
      val expectedPoints = Array(
        Array(44, -71), Array(45, -71), Array(46, -71),
        Array(44, -70), Array(45, -70), Array(46, -70),
        Array(44, -69), Array(45, -69), Array(46, -69)
      )

      assertLatLonArrayEqual(points, expectedPoints)
    }

    it("should produce proper squares with negative lat/lon") {
      val points = Geo.getUnitPoints(-45.587, -70.321, 100)
      val expectedPoints = Array(
        Array(-44, -71), Array(-45, -71), Array(-46, -71),
        Array(-44, -70), Array(-45, -70), Array(-46, -70),
        Array(-44, -69), Array(-45, -69), Array(-46, -69)
      )

      assertLatLonArrayEqual(points, expectedPoints)
    }

    it("should produce oblong number of squares near the poles when the radius is larger then longitudinal distance") {
      val points = Geo.getUnitPoints(75.123, -30.987, 60000)
      val expectedPoints = Array(
        Array(74, -27), Array(75, -27), Array(76, -27),
        Array(74, -28), Array(75, -28), Array(76, -28),
        Array(74, -29), Array(75, -29), Array(76, -29),
        Array(74, -30), Array(75, -30), Array(76, -30),
        Array(74, -31), Array(75, -31), Array(76, -31),
        Array(74, -32), Array(75, -32), Array(76, -32),
        Array(74, -33), Array(75, -33), Array(76, -33)
      )

      assertLatLonArrayEqual(points, expectedPoints)
    }
  }

  describe("Geo degToRad Tests") {
    it("should properly convert degrees to radians") {
      assert(Geo.degToRad(0) == 0)
      assert(Geo.degToRad(-90) == -math.Pi / 2)
      assert(Geo.degToRad(180) == math.Pi)
      assert(Geo.degToRad(-270) == -3 * math.Pi / 2)
      assert(Geo.degToRad(360) == 2 * math.Pi)
    }
  }

  describe("Geo distanceBetween Tests") {
    it("should properly calculate the distance between points") {
      assert(Geo.distanceBetween(34.567, -12.345, 87.45, 76.432).toInt == 6162164)
      assert(Geo.distanceBetween(0, -12.345, 87.45, 0).toInt == 9730556)
      assert(Geo.distanceBetween(0, 0, 0, 0).toInt == 0)
    }
  }
}
