package sample.deltalake

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaLakeExample {

  /**Will create spark session.
    *
    * @return SparkSession
    */
  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DeltaLakeExample")
      .getOrCreate()
  }

  /**Will create data frame by reading csv file also will add extra column in the data frame.
    *
    * @param path CSV file path.
    * @return DataFrame.
    */
  private def createDataFrameFromFile(path: String): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    //implicits imports.
    import sparkSession.implicits._
    //data frame from the file.
    val dataFrame = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
    //Added new column name Expensive if price > 100 then Yes else No.
    val columnCondition = when($"Price".gt(100), "Yes")
      .otherwise("No")
    dataFrame.withColumn(
      "Expensive",
      columnCondition
    )
  }

  /**This function will read the data frame created by createDataFrameFromFile and will store
    * the data in specified folder
    *
    */
  private def writeParquetFile: Unit = {
    val dataFrame = createDataFrameFromFile("src/main/resources/colors.csv");
    dataFrame.write
    //Delta lake will store the data in parquet format.
      .format("delta")
      .save("src/main/resources/DeltaLake")
  }

  def main(args: Array[String]): Unit = {
    //Setting log level to error.
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creating spark session.
    val sparkSession = createSparkSession
    //Writing parquet file.
    writeParquetFile
    //closing session.
    sparkSession.close()
  }
}
