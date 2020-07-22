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

    /**
      *
      * Select
      * Id,
      * Upper(Name) as Name,
      * Age,
      * Case Gender
      * When 'M' Then
      * 'Male'
      * Else
      * 'Female'
      * End As Gender
      * From
      * Students;
      * Above Query equivalent logic
      *
      */
    val selectedColumnDataFrame =
      tableDataFrame.select(
        $"ID",
        upper(tableDataFrame("NAME")) as "NAME",
        $"AGE",
        when(tableDataFrame("GENDER") === "M", "Male")
          .when(tableDataFrame("GENDER") === "F", "Female")
          .otherwise("NotSpecified") as "Gender"
      )
    //Printing top 20 rows
    selectedColumnDataFrame.show()
  }
}
