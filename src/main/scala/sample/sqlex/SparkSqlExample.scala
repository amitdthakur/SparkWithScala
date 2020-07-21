package sample.sqlex

import org.apache.spark.sql.SparkSession

object SparkSqlExample {
  def main(args: Array[String]): Unit = {
    //Creating spark session object.
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSqlExample")
      .master("local[*]")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    val fileDataFrame = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("../files/csv/CitiGroup2006_2008.csv")
    import sparkSession.implicits._
    //Printing rows from the csv
    //fileDataFrame.select( $"close",$"close" + 1 as("CloseIncremented")).show()
    //Filtering data
    fileDataFrame.filter($"close" >= 489.9).show()
    //Group by
    fileDataFrame.groupBy("close").count().show()
    //Register data frame as table.
    fileDataFrame.createGlobalTempView("employee")
    //Querying table
    val sparkSqlTable = sparkSession.sql("Select * from employee")
    //Printing result
    sparkSqlTable.show()

  }
}
