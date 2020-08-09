package sample.json

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataSetJsonExample {
  //case class
  private case class Student(
      name: Option[String],
      age: Long,
      address: String,
      jobTitle: String
  )

  //Complex type
  private case class Students(students: Array[Student])

  def main(args: Array[String]): Unit = {
    //Setting log level to error
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataSetJsonExample")
      .getOrCreate()
    val dataFrame = sparkSession.read
    //As json spread across multiple lines
      .option("multiline", "true")
      .json("src/main/resources/Sample.json")
    import sparkSession.implicits._
    val datasetOfJson = dataFrame.as[Student]
    //printing data frame values
    datasetOfJson.show()
  }

}
