package io.xmode.kruthar.mobilelocationidentifier

case class Config(
                 mobileDataPath: Option[String] = None,
                 locationDataPath: Option[String] = None,
                 startTime: Option[Long] = None,
                 endTime: Option[Long] = None
                 ) {
  lazy val mobilePath: String = {
    mobileDataPath match {
      case Some(path) if path.endsWith(".parquet") => path
      case Some(path) if path.endsWith("/") => s"$path*.parquet"
      case Some(path) => s"$path/*.parquet"
      case _ => throw new IllegalStateException("mobileDataPath is empty, this field is required.")
    }
  }

  lazy val locationPath: String = {
    locationDataPath match {
      case Some(path) if path.endsWith(".csv") => path
      case Some(path) if path.endsWith("/") => s"$path*.csv"
      case Some(path) => s"$path/*.csv"
      case _ => throw new IllegalStateException("locationDataPath is empty, this field is required.")
    }
  }

  lazy val start: Long = startTime.getOrElse(Long.MinValue)

  lazy val end: Long = endTime.getOrElse(Long.MaxValue)
}