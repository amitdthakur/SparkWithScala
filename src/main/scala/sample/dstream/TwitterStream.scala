package sample.dstream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterStream {

  /**Returns spark session object.
    *
    *
    * @return SparkSession SparkSession
    */
  private def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("TwitterStream")
      .getOrCreate()
  }

  private def callTwitter(
      streamingContext: StreamingContext
  ): DStream[(String, Int)] = {
    // Now let's wrap the context in a streaming one, passing along the window size
    val tweets = TwitterUtils.createStream(streamingContext, None)
    val wordsDStream = tweets.map(_.getText).flatMap(_.split(" "))
    //Checking only for hash tags
    val hashTagsDStream = wordsDStream.filter(_.startsWith("#"))
    val mappedColumn = hashTagsDStream.map(x => (x, 1))
    mappedColumn.reduceByKeyAndWindow(_ + _, _ - _, Seconds(500), Seconds(10))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkContext = createSparkSession.sparkContext
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))
    val flatMappedTexts = callTwitter(streamingContext)
    flatMappedTexts.print()
    //Checkpoint directory
    streamingContext.checkpoint("D:\\files")
    // Now that the streaming is defined, start it
    streamingContext.start()
    // Waiting for the tweets for time specified in below function
    streamingContext.awaitTermination()
  }

}
