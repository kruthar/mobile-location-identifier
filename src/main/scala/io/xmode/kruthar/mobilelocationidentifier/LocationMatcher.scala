package io.xmode.kruthar.mobilelocationidentifier

//import org.apache.spark.sql.SparkSession

object LocationMatcher {
  def main(args: Array[String]): Unit = {
    val config = ArgParser.parse(args)

//    val spark = SparkSession.builder.getOrCreate

    println(config)
  }
}

