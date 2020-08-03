package sample.sql

import org.apache.spark.sql.functions.{count, max, min}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlDataFrameExample {

  private def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSqlDataFrameExample")
      .getOrCreate()
  }

  //Post case class
  case class Posts(authorId: Int, subForum: String, likes: Int)
  private def createDataFrameWithHardCodedDataForPosts(): DataFrame = {
    val sparkSession = createSparkSession()
    import sparkSession.implicits._
    sparkSession.sparkContext
      .parallelize(
        Seq(
          Posts(1, "design", 2),
          Posts(1, "debate", 0),
          Posts(2, "debate", 0),
          Posts(3, "debate", 23),
          Posts(1, "design", 1)
        )
      )
      .toDF()
  }

  //Most expensive expensive home per zip code.
  private def eachAuthorsPostSubForm(): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val dataFrameWithData = createDataFrameWithHardCodedDataForPosts()
    import sparkSession.implicits._
    val selectedRdd = dataFrameWithData.groupBy($"subForum", $"authorId")
    val countRdd = selectedRdd.agg(count($"authorId"))
    countRdd.sort($"subForum", $"count(authorId)".desc)
  }

  //Creating case class.
  case class Listing(street: String, zip: Int, price: Long)

  private def createDataFrameWithHardCodedData(): DataFrame = {
    val sparkSession = createSparkSession()
    import sparkSession.implicits._
    sparkSession.sparkContext
      .parallelize(
        Seq(
          Listing("MGRoad", 410206, 4600000L),
          Listing("MGRoad", 410206, 5600000L),
          Listing("BankStand", 400001, 1000000000L),
          Listing("MarineLines", 400003, 1500000000L),
          Listing("MarineLines", 400003, 200000000000L)
        )
      )
      .toDF()
  }

  //Most expensive expensive home per zip code.
  private def mostExpensiveHomePerZipCode(): DataFrame = {
    val dataFrameWithData = createDataFrameWithHardCodedData()
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val groupByDataFrame = dataFrameWithData.groupBy($"street")
    groupByDataFrame.agg(max($"price"))
  }
  //Least expensive expensive home per zip code.
  private def leastExpensiveHomePerZipCode(): DataFrame = {
    val dataFrameWithData = createDataFrameWithHardCodedData()
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val groupByDataFrame = dataFrameWithData.groupBy($"street")
    groupByDataFrame.agg(min($"price"))
  }
  def main(args: Array[String]): Unit = {
    val sparkSession = createSparkSession()
    val dataFrame = eachAuthorsPostSubForm()
    dataFrame.show()
    sparkSession.close()
  }

}
