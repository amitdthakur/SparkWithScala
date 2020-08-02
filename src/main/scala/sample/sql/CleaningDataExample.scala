package sample.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**This class consist functions to clean the data from given data frame.
  *
  */
object CleaningDataExample {

  //Creating spark session.
  private def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("CleaningDataExample")
      .getOrCreate()
  }

  private def createDataFrameFromFile(): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/ContainsNull.csv")
  }

  private def dropAllRowsConsistNull(): DataFrame = {
    val dataFrameWithCleanUpData = createDataFrameFromFile()
    dataFrameWithCleanUpData.na.drop()
  }

  private def dropAllRowsIfAllRowIsNull(): DataFrame = {
    val dataFrameWithCleanUpData = createDataFrameFromFile()
    dataFrameWithCleanUpData.na.drop("all")
  }

  private def dropAllRowsForParticularColumns(): DataFrame = {
    val dataFrameWithCleanUpData = createDataFrameFromFile()
    dataFrameWithCleanUpData.na.drop(Array("Id", "Sales"))
  }

  private def fillTheParticularValue(): DataFrame = {
    val dataFrameWithCleanUpData = createDataFrameFromFile()
    dataFrameWithCleanUpData.na.fill(12)
  }

  private def fillTheParticularValueForColumn(): DataFrame = {
    val dataFrameWithCleanUpData = createDataFrameFromFile()
    dataFrameWithCleanUpData.na.fill(0, Array("Id"))
  }

  private def fillTheParticularValueForColumnUsingMap(): DataFrame = {
    val dataFrameWithCleanUpData = createDataFrameFromFile()
    dataFrameWithCleanUpData.na.fill(Map("id" -> 0))
  }

  private def fillTheParticularValueForColumnUsingArray(): DataFrame = {
    val dataFrameWithCleanUpData = createDataFrameFromFile()
    dataFrameWithCleanUpData.na.replace(Array("id"), Map(1 -> 5))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = createSparkSession()
    val dataFrame = fillTheParticularValueForColumnUsingArray
    dataFrame.show()
    //filling number in place of sales column

    sparkSession.close()
  }

}
