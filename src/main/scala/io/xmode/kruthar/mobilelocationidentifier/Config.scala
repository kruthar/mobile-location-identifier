package io.xmode.kruthar.mobilelocationidentifier

case class Config(
                 mobileDataPath: Option[String] = None,
                 locationDataPath: Option[String] = None,
                 startTime: Option[Long] = None,
                 endTime: Option[Long] = None
                 )
