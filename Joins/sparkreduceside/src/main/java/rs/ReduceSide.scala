package rs

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.Encoders

object ReduceSide {

  case class TwitterData(Follower: Long, Followed: Long)

  /**
   * This is the main driver program.
   *
   * @param args represents command line arguments that represent the input and output folders
   */
  def main(args: Array[String]) {
    var logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntf.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TriangleCount")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val maxFilter = 40000

    val leftRDD = textFile.filter(data => (data.split(",")(0).toInt <= maxFilter && data.split(",")(1).toInt <= maxFilter)).map(line => (line.split(",")(0), line.split(",")(1)))
    val rightRDD = textFile.filter(data => (data.split(",")(0).toInt <= maxFilter && data.split(",")(1).toInt <= maxFilter)).map(line => (line.split(",")(1), line.split(",")(0)))

        reduceSideUsingRDD(leftRDD, rightRDD, args)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    reduceSideUsingDataSet(args, sqlContext, textFile, logger, maxFilter);
  }

  /**
   * This method represents a reduce side join (Hash+Shuffle) for RDD
   */
  def reduceSideUsingRDD(leftRDD: RDD[(String, String)], rightRDD: RDD[(String, String)], args: Array[String]) {
    val rdd = leftRDD.join(rightRDD).map(data => (data._2._1, data._2._2))
    val count = rdd.join(leftRDD).filter(data => (data._2._1.equals(data._2._2))).count()

    System.out.println("Triangle Count with MAX_FILTER: 40000" +  ":" + count / 3);
    println("Triangle Count With MAX_FILTER: 40000" + " : " + count / 3)
  }


  /**
   * This method is used to generate the Triangle count using DataSets.
   *
   * @param args
   * @param sqlContext
   * @param textFile
   * @param logger
   * @param maxFilter
   */
  def reduceSideUsingDataSet(args: Array[String], sqlContext: SQLContext, textFile: RDD[String], logger: Logger, maxFilter: Int) {
    import sqlContext.implicits._

    val leftDS = textFile.filter(data => data.split(",")(0).toInt <= maxFilter && data.split(",")(1).toInt <= maxFilter).map(line => TwitterData(line.split(",")(0).toLong, line.split(",")(1).toLong)).toDS()
    val rightDS = textFile.filter(data => data.split(",")(0).toInt <= maxFilter && data.split(",")(1).toInt <= maxFilter).map(line => TwitterData(line.split(",")(1).toLong, line.split(",")(0).toLong)).toDS()

    val joinedDS = leftDS.join(rightDS, "Follower").map(data => TwitterData(data.getLong(1), data.getLong(2)))
    joinedDS.explain()

    val count = joinedDS.join(leftDS, "Follower").filter(data => (data.getLong(1) == data.getLong(2))).count()

    System.out.println("Triangle Count with MAX_FILTER:" + maxFilter + ":" + count / 3);
    println("Triangle Count With MAX_FILTER: " + maxFilter + " : " + count / 3)

  }
}
