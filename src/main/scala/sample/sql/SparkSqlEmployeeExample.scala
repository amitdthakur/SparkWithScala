package sample.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSqlEmployeeExample {

  //case class
  case class Employee(id: Int,
                      fname: String,
                      lname: String,
                      age: Int,
                      city: String)
  private def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSqlEmployeeExample")
      .getOrCreate()
  }

  private def createDataFrame(): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    sparkSession.sparkContext
      .parallelize(
        List(
          Employee(12, "Amit", "Thakur", 28, "Panvel"),
          Employee(13, "Akshaya", "Sawant", 25, "Mumbai"),
          Employee(14, "Stranger", "StrangerLname", 14, "Mumbai"),
          Employee(15, "Diana", "Almida", 29, "Stockholm")
        )
      )
      .toDF()
  }

  private def queryEmployeeTable(): DataFrame = {
    val dataFrame = createDataFrame()
    val sparkSession = SparkSession.builder().getOrCreate()
    dataFrame.createOrReplaceTempView("Employee")
    sparkSession.sql(
      "Select id,lname from Employee where city='Mumbai' order by id desc"
    )
  }

  private def queryEmployeeDatFrameApi(): Dataset[Row] = {
    val dataFrame = createDataFrame()
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    //Selecting particular columns
    val selectedColumnsDataFrame = dataFrame.select($"id", $"lname")
    //filtering data based on criteria
    val mumbaiCityDataFrame =
      selectedColumnsDataFrame.where($"city".equalTo("Mumbai"))
    //Sorting based on the columns.
    mumbaiCityDataFrame.orderBy($"id")
  }

  def main(args: Array[String]): Unit = {
    //Setting logger level to Error
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creating spark session
    createSparkSession()
    //Creating data frame
    val dataFrame = queryEmployeeDatFrameApi()
    dataFrame.show()
  }

}
