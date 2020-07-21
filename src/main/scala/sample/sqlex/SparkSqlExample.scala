package sample.sqlex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//case class for mapping
case class CsvRow(Date: String,
                  Open: Double,
                  High: Double,
                  Low: Double,
                  Close: Double,
                  Volume: Integer)

object SparkSqlExample {

  /**This function will increment the high column value by 1.
    *
    * @param line CsvRow case class
    * @return Double
    */
  def mapperForHighIncrementation(line: CsvRow): Double = {
    line.High + 1
  }

  def main(args: Array[String]): Unit = {
    //Creating spark session object.
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSqlExample")
      .master("local[*]")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._
    val fileDataFrame = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/CitiGroup2006_2008.csv")
    //Printing rows from the csv
    fileDataFrame.select($"close", $"close" + 1 as ("CloseIncremented")).show()
    //Filtering data
    //fileDataFrame.filter($"close" >= 489.9).show()
    //Group by
    //fileDataFrame.groupBy("close").count().show()
    //Register data frame as table.
    fileDataFrame.createOrReplaceTempView("employee")
    //Querying tablel
    val sparkSqlTable = sparkSession.sql("Select * from employee")
    //Printing result
    //sparkSqlTable.show()
    //Creating global temporary table.
    fileDataFrame.createGlobalTempView("employeeGlobal")
    val sparkGlobalTable =
      sparkSession.sql("select * from global_temp.employeeGlobal")
    //sparkGlobalTable.show()
    //converting to data set
    val dataset = fileDataFrame.as[CsvRow]
    //dataset.show()
    println("New data set ")
    //Added 1 in high column
    val alteredDataSet = dataset.map(mapperForHighIncrementation)
    //Pulling year from the date and printing time stamp
    val selectedColumn = dataset.select(
      year($"Date" as "OnlyYear"),
      current_timestamp() as "TimeStamp"
    )
    selectedColumn.show()
  }
}
