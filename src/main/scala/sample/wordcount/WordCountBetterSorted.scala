package sample.wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/** Consist function to count the words from given input file.
  *
  */
object WordCountBetterSorted {

  /**Returns spark session object.
    *
    *
    * @return SparkSession SparkSession
    */
  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCountBetterSorted")
      .getOrCreate()
  }

  /**Returns word count by reading whole file.
    *
    * @param filePath File path for which words need to be counted.
    * @return RDD consist word, count
    */
  private def getWordCountFromTextFile(filePath: String): RDD[(String, Int)] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val dataFrameReader = sparkSession.read
      .textFile(filePath)
    val flatten = dataFrameReader.rdd.flatMap(x => x.split("\\W+"))
    val lowerCaseRdd = flatten.map(_.toLowerCase())
    lowerCaseRdd.map(x => (x, 1)).reduceByKey(_ + _)
  }
  //Main function.
  def main(args: Array[String]): Unit = {
    //Set log level to Error.
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creating spark session
    createSparkSession
    //get the word count
    val countOfWords = getWordCountFromTextFile("src/main/resources/book.txt")
    countOfWords.foreach(println)
  }

}
