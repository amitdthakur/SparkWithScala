package sample.rangpartitioner

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{RangePartitioner, SparkContext}

object RangePartitionExample {

  private case class Student(name: String, id: Int)

  private def createSparkContext(): SparkContext = {
    SparkSession.builder().master("local[12]").getOrCreate().sparkContext
  }

  private def createPairRdd(): RDD[(String, Iterable[Int])] = {
    val sparkContext = createSparkContext()
    val rdd = sparkContext.parallelize(
      Seq(Student("Amit", 12), Student("Akshaya", 13), Student("Stranger", 14))
    )
    val mappedRddPair =
      rdd.map(student => (student.name, student.id))
    //Creating range partition
    val rangePartition = new RangePartitioner(3, mappedRddPair);
    //Persisting
    val partitionedRdd =
      mappedRddPair.partitionBy(rangePartition).persist()
    partitionedRdd.groupByKey()
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    createPairRdd.foreach(println)
  }

}
