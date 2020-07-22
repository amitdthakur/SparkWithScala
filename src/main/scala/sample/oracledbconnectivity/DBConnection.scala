package sample.oracledbconnectivity

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{upper, when}

/**Class consist logic for connecting data base.
  *
  */
object DBConnection {
  //Main function
  def main(args: Array[String]): Unit = {
    //Creating spark session object
    val sparkSession = SparkSession
      .builder()
      .appName("DBConnection")
      .master("local[*]")
      .getOrCreate()
    //Connecting to respective data base and table and returning data frame
    val tableDataFrame = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@LAPTOP-LG3DSMSM:1521:xe")
      .option("user", "admin")
      .option("password", "admin")
      .option("dbtable", "students")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    import sparkSession.implicits._
    //Filtering based on age <28
    val filteredDataFrameBasedOnAge = tableDataFrame.where('Age.lt(28))
    //Converting Gender column from M to Male F to Female
    val genderColumnDataSet = filteredDataFrameBasedOnAge.withColumn(
      "Gender",
      when('Gender.equalTo("M"), "Male")
        .when('Gender.equalTo("F"), "Female")
        .otherwise("NotSpecified") as 'Gender
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
      genderColumnDataSet.select('Id, upper('Name) as 'NAME, 'Age, 'Gender)
    //Printing top 20 rows
    selectedColumnDataFrame.show()
  }
}
