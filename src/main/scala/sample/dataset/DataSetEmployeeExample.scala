package sample.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object DataSetEmployeeExample {

  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataSetEmployeeExample")
      .getOrCreate()
  }
  //Creating case class.
  case class Listing(street: String, zip: Int, price: Long)

  private def createDataSetWithHardCodedData: Dataset[Listing] = {
    val sparkSession = SparkSession.builder().getOrCreate()
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
      .toDS()
  }

  private def createDataSetWithHardCodedDataKeyValue: Dataset[(Int, String)] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    List((1, "A"), (2, "B"), (3, "C"), (1, "A"))
      .toDS()
  }

  private def createReduceOperation(
      dataSet: Dataset[(Int, String)]
  ): Dataset[(Int, String)] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    dataSet
      .groupByKey(_._1)
      .mapGroups((k, v) => (k, v.foldLeft("")((acc, p) => acc + p._2)))
  }

  private def createReduceOperationUsingReduceGroups(
      dataSet: Dataset[(Int, String)]
  ): Dataset[(Int, String)] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    dataSet
      .groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups((k, v) => k + v)
  }

  private def reducingUsingAggregatorClass()
      : TypedColumn[(Int, String), String] = {
    new Aggregator[(Int, String), String, String] {
      //Default value
      def zero: String = ""
      //Reduce operation
      def reduce(b: String, a: (Int, String)): String = b + a._2
      //Merging operation
      def merge(b: String, a: String): String = b + a
      //No operation before finish
      def finish(r: String): String = r
      override def bufferEncoder: Encoder[String] = Encoders.STRING
      override def outputEncoder: Encoder[String] = Encoders.STRING
    }.toColumn
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = createSparkSession
    import sparkSession.implicits._
    //val dataSet=createDataSetWithHardCodedData
    //Converting it to typed column.
    //val grouped=dataSet.groupByKey(_.price).agg(avg($"price").as[Long])
    //grouped.show()
    val dataset = createDataSetWithHardCodedDataKeyValue
    val aggregator = reducingUsingAggregatorClass
    createDataSetWithHardCodedDataKeyValue
      .groupByKey(_._1)
      .agg(aggregator.as[String])
      .show()
    //closing the spark session
    sparkSession.close()
  }
}
