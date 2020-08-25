package sample.datasourcewriting

import org.apache.spark.sql.{DataFrame, SparkSession}

object TableWritingExample {
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
      .option("header", "true")
      .load(path)
  }

  private def writeAsTable: Unit = {
    val dataFrame = createDataFrameOutOfFilePath(
      "src/main/resources/CitiGroup2006_2008.csv"
    )
    dataFrame.write
      .saveAsTable("abc")
  }

  private def writeAsPartitionBy: Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val dataFrame = createDataFrameOutOfFilePath(
      "src/main/resources/colors.csv"
    )
    dataFrame.write
    //partitionBy column
      .partitionBy("color")
      .format("csv")
      .save("D:\\files\\colors")
  }

  private def writeAsPartitionByBucketBy: Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val dataFrame = createDataFrameOutOfFilePath(
      "src/main/resources/colors.csv"
    )
    dataFrame.write
    //partitionBy column
      .partitionBy("color")
      .bucketBy(2, "color")
      .saveAsTable("colors")
  }
  def main(args: Array[String]): Unit = {
    val sparkSession = createSparkSession
    writeAsPartitionByBucketBy
    sparkSession.close()
  }
}
