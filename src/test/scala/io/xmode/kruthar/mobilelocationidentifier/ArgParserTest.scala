package io.xmode.kruthar.mobilelocationidentifier

import org.scalatest.FunSpec

class ArgParserTest extends FunSpec {
  describe("ArgParser Tests") {
    it("should throw exception for malformed parameters") {
      val keyonly = intercept[IllegalArgumentException] {
        ArgParser.parse(Array("start=0", "end"))
      }
      assert(keyonly.getMessage == "Malformed parameter [end] should be key=value.")

      val toomany = intercept[IllegalArgumentException] {
        ArgParser.parse(Array("start=0=something", "end=1"))
      }
      assert(toomany.getMessage == "Malformed parameter [start=0=something] should be key=value.")
    }

    it("should throw exception for unexpected parameters") {
      val unexpected = intercept[IllegalArgumentException] {
        ArgParser.parse(Array("key=value", "start=0"))
      }
      assert(unexpected.getMessage == "Unexpected parameter [key=value].")
    }

    it("should throw exception for bad timestamps") {
      val badstart = intercept[IllegalArgumentException] {
        ArgParser.parse(Array("start=bad"))
      }
      assert(badstart.getMessage == "Bad start time [start=bad], expecting a valid millisecond timestamp.")

      val badend = intercept[IllegalArgumentException] {
        ArgParser.parse(Array("end=bad"))
      }
      assert(badend.getMessage == "Bad end time [end=bad], expecting a valid millisecond timestamp.")
    }

    it("should identify all given args") {
      val args = Array(
        "mobile=/path/to/dir/",
        "locations=/path/to/other/dir/",
        "output=/path/to/output/",
        "outformat=parquet",
        "start=1605657600000",
        "end=1605744000000"
      )
      val config = Config(
        Some("/path/to/dir/"),
        Some("/path/to/other/dir/"),
        Some("/path/to/output/"),
        Some("parquet"),
        Some(1605657600000L),
        Some(1605744000000L)
      )
      assert(ArgParser.parse(args) === config)
    }

    it("should successfully parse without timestamps") {
      val args = Array(
        "mobile=/path/to/dir/",
        "locations=/path/to/other/dir/"
      )
      val config = Config(
        Some("/path/to/dir/"),
        Some("/path/to/other/dir/"),
        None,
        None,
        None,
        None
      )
      assert(ArgParser.parse(args) === config)
    }

    it("should throw exception if missing data field parameters") {
      val missingmobile = intercept[IllegalStateException] {
        ArgParser.parse(Array("locations=/path/to/data"))
      }
      assert(missingmobile.getMessage == "mobile and locations data paths are required fields.")

      val missinglocations = intercept[IllegalStateException] {
        ArgParser.parse(Array("mobile=/path/to/data"))
      }
      assert(missinglocations.getMessage == "mobile and locations data paths are required fields.")

      val missingboth = intercept[IllegalStateException] {
        ArgParser.parse(Array())
      }
      assert(missingboth.getMessage == "mobile and locations data paths are required fields.")
    }

    it("should throw exception if start is after end") {
      val startafterend = intercept[IllegalStateException] {
        ArgParser.parse(Array("mobile=path", "locations=path", "start=5", "end=4"))
      }
      assert(startafterend.getMessage == "start time must come before end time.")
    }
  }
}
