package sample.oraclesql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{upper, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**Class consist logic for connecting data base.
  *
  */
object OracleSqlResultDataFrame {

  /**Function to create spark session.
    *
    *
    * @return SparkSessionSparkSession
    */
  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("OracleSqlResultDataFrame")
      .master("local[*]")
      .getOrCreate()
  }

  /**Creates data frame for the respective table.
    *
    *
    * @param sparkSession Spark session
    * @param tableName Table name
    * @return DataFrame for that table.
    */
  def connectToDataBaseAndGetTheDataFrame(sparkSession: SparkSession,
                                          tableName: String): DataFrame = {
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
    val sparkSession = createSparkSession()
    //Connecting to respective data base and student and returning data frame.
    val studentsTableDataFrame =
      connectToDataBaseAndGetTheDataFrame(sparkSession, "students")
    //Implicits imports for handling $
    import sparkSession.implicits._
    //Filtering based on age <28
    val filteredDataFrameBasedOnAgeDataFrame =
      studentsTableDataFrame.where($"Age".lt(28))
    //Converting Gender column from M to Male F to Female
    val genderColumnDataFrame = filteredDataFrameBasedOnAgeDataFrame.withColumn(
      "GENDER",
      when($"Gender".equalTo("M"), "Male")
        .when($"Gender".equalTo("F"), "Female")
        .otherwise("NotSpecified")
    )

    /**
      *
      * SELECT
      * ID,
      * UPPER(NAME)     AS NAME,
      * age,
      * CASE gender
      * WHEN 'M'  THEN
      * 'Male'
      * WHEN 'F'  THEN
      * 'Female'
      * ELSE
      * 'NotSpecified'
      * END             AS gender
      * FROM
      * students
      * where age<28;
      *
      */
    val selectedColumnDataFrame =
      genderColumnDataFrame.select(
        $"ID",
        upper($"Name") as "NAME",
        $"AGE",
        $"GENDER"
      )
    //Printing top 20 rows
    selectedColumnDataFrame.show()
    //Explain the query.
    //println(selectedColumnDataFrame.explain(true))
    //Closing spark session
    sparkSession.close()
  }
}
