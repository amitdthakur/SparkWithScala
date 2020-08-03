package sample.pairrdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**This class consist function to perform join operation on pair RDD.
  *
  */
object JoinOnPairRddExample {

  private def createSparkContext: SparkContext = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("JoinOnPairRddExample")
      .getOrCreate()
      .sparkContext
  }
  //case class.
  case class Employee(name: String, age: Int, numberOfFriends: Int)

  private def pairRddByCaseClass(): RDD[(String, (Int, Int))] = {
    val sparkContext = createSparkContext
    val rdd = sparkContext.parallelize(
      List(
        Employee("Amit", 28, 12),
        Employee("Akshaya", 25, 13),
        Employee("Stranger", 38, 18),
        Employee("Stranger", 39, 25)
      )
    )
    rdd
      .map(employee => (employee.name, (1, employee.numberOfFriends)))
  }

  def main(args: Array[String]): Unit = {
    //Setting logger level.
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkContext = createSparkContext
    val namesRdd = sparkContext.parallelize(
      List(
        (101, ("Amit", "AG")),
        (102, ("Akshaya", "DAG")),
        (103, ("Stranger", "DAG"))
      )
    )
    val locations = sparkContext.parallelize(
      List((101, "Vastra Skongen"), (102, "Solna Centrum"))
    )
    val trackedCustomers = namesRdd.leftOuterJoin(locations)
    trackedCustomers.foreach(println)
  }

}
