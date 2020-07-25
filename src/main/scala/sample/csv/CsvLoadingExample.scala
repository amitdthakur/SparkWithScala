package sample.csv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvLoadingExample {

  /**Function to create spark session.
    *
    *
    * @return SparkSessionSparkSession
    */
  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("CsvLoading")
      .getOrCreate()
  }

  /**Creates data frame for the respective input file.
    *
    *
    * @param filePath  File path.
    * @return DataFrame for that table.
    */
  private def getDataFrameOfFile(filePath: String): DataFrame = {
    SparkSession.builder().getOrCreate()
    val sparkSession = SparkSession.builder().getOrCreate()
    sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .format("csv")
      .load(filePath)
  }

  def main(args: Array[String]): Unit = {
    //Setting logger level to error to get rid of Info logs.
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = createSparkSession
    val dataFrameReader = getDataFrameOfFile(
      "src/main/resources/clean_usa_housing.csv"
    )
    import sparkSession.implicits._
    dataFrameReader.agg(max($"Price")).show()
  }
}
