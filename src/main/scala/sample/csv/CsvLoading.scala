package sample.csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object CsvLoading {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CsvLoading")
      .getOrCreate()
    val dataFrameReader = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .format("csv")
      .load("../files/clean_usa_housing.csv")
    import sparkSession.implicits._
    dataFrameReader.agg(max($"Price")).show()
  }
}
