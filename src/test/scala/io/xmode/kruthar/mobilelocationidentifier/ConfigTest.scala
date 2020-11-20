package io.xmode.kruthar.mobilelocationidentifier

import org.scalatest.FunSpec

class ConfigTest extends FunSpec {
  describe("Config object mobilePath tests") {
    it("should return an unaltered path for a parquet file") {
      val config = Config(Some("/path/to/file.parquet"), Some("locations"), None, None, None, None)
      assert(config.mobilePath === "/path/to/file.parquet")
    }

    it("should return proper parquet path for slash dir") {
      val config = Config(Some("/path/to/"), Some("locations"), None, None, None, None)
      assert(config.mobilePath === "/path/to/*.parquet")
    }

    it("should return proper parquet path for open dir") {
      val config = Config(Some("/path/to"), Some("locations"), None, None, None, None)
      assert(config.mobilePath === "/path/to/*.parquet")
    }

    it("should throw exception if mobileDataPath is empty") {
      val emptyPath = intercept[IllegalStateException] {
        Config().mobilePath
      }
      assert(emptyPath.getMessage === "mobileDataPath is empty, this field is required.")
    }
  }

  describe("Config object locationPath tests") {
    it("should return an unaltered path for a csv file") {
      val config = Config(Some("mobile"), Some("/path/to/file.csv"), None, None, None, None)
      assert(config.locationPath === "/path/to/file.csv")
    }

    it("should return proper csv path for slash dir") {
      val config = Config(Some("mobile"), Some("/path/to/"), None, None, None, None)
      assert(config.locationPath === "/path/to/*.csv")
    }

    it("should return proper parquet path for open dir") {
      val config = Config(Some("mobile"), Some("/path/to"), None, None, None, None)
      assert(config.locationPath === "/path/to/*.csv")
    }

    it("should throw exception if locationDataPath is empty") {
      val emptyPath = intercept[IllegalStateException] {
        Config().locationPath
      }
      assert(emptyPath.getMessage === "locationDataPath is empty, this field is required.")
    }
  }

  describe("Config object start/end tests") {
    it("should return the starttime if it is present") {
      assert(Config(None, None, None, None, Some(5L), Some(6L)).start === 5L)
    }

    it("should return the default if starttime is empty") {
      assert(Config(None, None, None, None, None, Some(6L)).start === Long.MinValue)
    }

    it("should return the endtime if it is present") {
      assert(Config(None, None, None, None, Some(5L), Some(6L)).end === 6L)
    }

    it("should return the default if endtime is empty") {
      assert(Config(None, None, None, None, Some(5L), None).end === Long.MaxValue)
    }
  }
}
