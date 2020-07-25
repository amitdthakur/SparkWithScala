package sample.oraclesql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**Consist functions for SQL join operation in Spark.
  *
  */
object OracleSqlJoinExample {

  /**Returns spark session object.
    *
    *
    * @return SparkSessionSparkSession
    */
  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("OracleSqlJoinExample")
      .getOrCreate()
  }

  /**Returns the data frame.
    *
    *
    * @param tableName Table name
    * @return DataFrame for that table.
    */
  private def connectToDataBaseAndGetTheDataFrame(
    tableName: String
  ): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    sparkSession.read
      .format("jdbc")
      .option("url", "")
      .option("user", "admin")
      .option("password", "admin")
      .option("dbtable", tableName)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
  }
  //Main function
  def main(args: Array[String]): Unit = {
    //Setting logger level to error to get rid of Info logs.
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creating spark session.
    val sparkSession = createSparkSession
    //Connecting to respective data base and student and returning data frame.
    val studentsTableDataFrame =
      connectToDataBaseAndGetTheDataFrame("students")
    //Connecting to respective data base and phones and returning data frame.
    val phonesTableDataFrame =
      connectToDataBaseAndGetTheDataFrame("phones")
    //Right outer join can be denoted as right, rightouter, right_outer
    val rightOuterJoinDataFrame = studentsTableDataFrame
      .join(
        phonesTableDataFrame,
        studentsTableDataFrame("ID") === phonesTableDataFrame("ID"),
        "rightouter"
      )
    //Printing right outer join result
    rightOuterJoinDataFrame.show()
    //Closing spark session
    sparkSession.close()
  }
}
