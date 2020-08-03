package sample.pairrdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TransformationOnPairRddExample {

  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("TransformationOnPairRddExample")
      .getOrCreate()
  }
  private def createRddFromFile(filePath: String): RDD[String] = {
    val sparkSession = createSparkSession
    sparkSession.sparkContext.textFile(filePath)
  }

  private def mapperToGenerateTuple(line: String): (Int, Int) = {
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (age, numFriends)
  }

  def averageNumberOfFriendsForAge(): RDD[(Int, Int)] = {
    val rddFromFile = createRddFromFile("src/main/resources/fakefriends.csv")
    val rddWithTuple =
      rddFromFile.map(mapperToGenerateTuple).mapValues(v => (v, 1))
    val totalNumberOfAgeAndNumberOfFriends = rddWithTuple.reduceByKey(
      (valueOne, valueTwo) =>
        (valueOne._1 + valueOne._2, valueTwo._1 + valueTwo._2)
    )
    totalNumberOfAgeAndNumberOfFriends.mapValues(value => value._1 / value._2)
  }

  def main(arguments: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val avgValues = averageNumberOfFriendsForAge()
    avgValues.foreach(println)
  }

}
