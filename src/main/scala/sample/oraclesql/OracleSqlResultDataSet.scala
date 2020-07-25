package sample.oraclesql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType

object OracleSqlResultDataSet {
  def main(args: Array[String]): Unit = {
    //Spark session object
    val sparkSession = SparkSession
      .builder()
      .appName("OracleSqlResultDataSet")
      .master("local[*]")
      .getOrCreate()

    //Connecting to data base and pull up the table
    var tableDataFrame = sparkSession.read
      .format("jdbc")
      .option("url", "")
      .option("user", "admin")
      .option("password", "admin")
      .option("dbtable", "students")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    val dfSchema = tableDataFrame.schema
    dfSchema.foreach { field =>
      field.dataType match {
        case t: DecimalType if t != DecimalType(38, 18) =>
          tableDataFrame = tableDataFrame.withColumn(
            field.name,
            col(field.name).cast(DecimalType(38, 18))
          )

      }
    }

  }

}
