package sample.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**Consist different ways to convert RDD to DataSet.
  *
  */
object RddToDataSet {

  /**Function to create spark session.
    *
    *
    * @return SparkSessionSparkSession
    */
  private def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("RddToDataSet")
      .getOrCreate()
  }
  //Case class
  private case class Person(name: String, id: Integer)

  /**Map the fields to person object.
    *
    * @param fields Fields of single row.
    * @return Person object.
    */
  private def mapLineToCsvRow(fields: Array[String]): Person = {
    Person(fields(0), fields(1).trim.toInt)
  }

  /**
    *
    * Main function of program.
    */
  def main(arguments: Array[String]): Unit = {
    //Set log level to Error.
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = createSparkSession()
    //Setting spark context.
    val sparkContext = sparkSession.sparkContext
    //implicits import
    import sparkSession.implicits._
    val dataFrameOfFile = sparkContext
      .textFile("src/main/resources/people.txt")
      .map(_.split(","))
      .map(mapLineToCsvRow)
      .toDF()
    //Creating view for querying purpose
    dataFrameOfFile.createOrReplaceTempView("People")
    //Querying table
    val peopleTableDatFrame = sparkSession.sql("Select * from People ")
    //Printing top result
    peopleTableDatFrame.show()
  }

}
