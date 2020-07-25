package sample.csv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.{DataFrame, SparkSession}

object CityGroupsCsvLoading {

  /**Function to create spark session.
    *
    *
    * @return SparkSessionSparkSession
    */
  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("CityGroupsCsvLoading")
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
    val fileDataFrame = getDataFrameOfFile(
      "src/main/resources/CitiGroup2006_2008.csv"
    )
    import sparkSession.implicits._
    //Extract year from date column
    val yearColumn = fileDataFrame
      .withColumn("Year", year($"Date"))
    //Select columns
    val selected = yearColumn
      .select("Open", "High", "Year")
    val openColumnSpecifiedValue = selected
      .filter($"Open".equalTo(490.0))
    openColumnSpecifiedValue.show()
  }

}
