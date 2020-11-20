package io.xmode.kruthar.mobilelocationidentifier

object ArgParser {
  /**
   * Parse the given args array into a Config object
   * @param args - Array[String] of application arguments
   * @return - Config of parsed arguments
   */
  def parse(args: Array[String]): Config = {
    var config = Config()
    for (arg <- args) {
      val parts = arg.trim.split("=")

      if (parts.length != 2) {
        throw new IllegalArgumentException(s"Malformed parameter [$arg] should be key=value.")
      }

      val key = parts(0)
      val value = parts(1)

      key match {
        case "mobile" => config = config.copy(mobileDataPath = Some(value))
        case "locations" => config = config.copy(locationDataPath = Some(value))
        case "output" => config = config.copy(outputPath = Some(value))
        case "outformat" => config = config.copy(outputFormat = Some(value))
        case "start" => try {
            config = config.copy(startTime = Some(value.toLong))
          } catch {
            case _: NumberFormatException => throw new IllegalArgumentException(s"Bad start time [$arg], expecting a valid millisecond timestamp.")
          }
        case "end" => try {
            config = config.copy(endTime = Some(value.toLong))
          } catch {
            case _: NumberFormatException => throw new IllegalArgumentException(s"Bad end time [$arg], expecting a valid millisecond timestamp.")
          }
        case _ => throw new IllegalArgumentException(s"Unexpected parameter [$arg].")
      }
    }

    validate(config)
  }

  /**
   * Validate fields of a Config object.
   * @param config - The Config object to validate.
   * @return - the Config if it is valid, otherwise exception will be thrown.
   */
  def validate(config: Config): Config = {
    if (config.mobileDataPath.isEmpty || config.locationDataPath.isEmpty) {
      throw new IllegalStateException("mobile and locations data paths are required fields.")
    }

    if (config.startTime.getOrElse(0L) > config.endTime.getOrElse(Long.MaxValue)) {
      throw new IllegalStateException("start time must come before end time.")
    }

    config
  }
}
