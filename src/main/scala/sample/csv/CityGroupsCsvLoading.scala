package sample.csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.year

object CityGroupsCsvLoading {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CityGroupsCsvLoading")
      .getOrCreate()

    val fileDataFrame = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/CitiGroup2006_2008.csv")

    val yearColumn = fileDataFrame
      .withColumn("Year", year(fileDataFrame("Date")))
    import sparkSession.implicits._
    val selected = yearColumn
      .select("Open", "High", "Year")
    val openColumnSpecifiedValue = selected
      .filter($"Open" === 490.0)

    openColumnSpecifiedValue.filter($"Open" === 490.0)

    //Number of records in csv file.
    println(fileDataFrame.rdd.getNumPartitions)

  }

}
