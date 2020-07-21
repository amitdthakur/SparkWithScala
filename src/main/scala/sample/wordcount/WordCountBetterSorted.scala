package sample.wordcount

import org.apache.spark.sql.SparkSession

object WordCountBetterSorted {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCountBetterSorted")
      .getOrCreate()
    val dataFrameReader = sparkSession.read
      .textFile("src/main/resources/book.txt")

    val flatten = dataFrameReader.rdd.flatMap(x => x.split("\\W+"))
    val lowerCaseRdd = flatten.map(x => x.toLowerCase())
    val reduced = lowerCaseRdd.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    reduced.foreach(println)
  }

}
