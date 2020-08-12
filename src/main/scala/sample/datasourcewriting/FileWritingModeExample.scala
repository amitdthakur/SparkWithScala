package sample.datasourcewriting

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object FileWritingModeExample {

  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("FileWritingModeExample")
      .getOrCreate()
  }

  private def createDataFrameOutOfFilePath(path: String): DataFrame = {
    val sparkSession = createSparkSession
    sparkSession.read
      .format("csv")
      .option("inferSchema", "true")
      .load(path)
  }

  private def writeFileDefault: Unit = {
    val dataFrame = createDataFrameOutOfFilePath(
      "src/main/resources/CitiGroup2006_2008.csv"
    )
    dataFrame.write
      .format("csv")
      //SaveMode.ErrorIfExists
      .save("D:\\files\\cityGroups")
  }

  private def writeFileAppend: Unit = {
    val dataFrame = createDataFrameOutOfFilePath(
      "src/main/resources/CitiGroup2006_2008.csv"
    )
    dataFrame.write
      .format("csv")
      //SaveMode.Append
      .mode(SaveMode.Append)
      .save("D:\\files\\cityGroups")
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = createSparkSession
    writeFileDefault
    sparkSession.close()
  }


}
