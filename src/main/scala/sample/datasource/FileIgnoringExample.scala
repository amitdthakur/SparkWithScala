package sample.datasource

import org.apache.spark.sql.SparkSession

object FileIgnoringExample {
  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("FileWritingModeExample")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = createSparkSession
    val testCorruptDF = sparkSession.read
      .format("csv")
      .option("inferSchema", "true")
      .option("pathGlobFilter", "*.csv")
      .load("D:\\files\\csv\\")
    testCorruptDF.show()

  }

}
