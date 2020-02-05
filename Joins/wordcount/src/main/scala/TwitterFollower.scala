
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This represents multiple implementations for saving the Followed node and the follower count data, using RDDs
 * DataSets and by using various aggregation methods.
 */
object TwitterFollower {

  def main(args: Array[String]) {
    var logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntf.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterFollower").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))

//    RDD_G(sc, args, textFile)
//    RDD_R(sc, args, textFile)
//    RDD_F(sc, args, textFile)
//    RDD_A(sc, args, textFile)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    D_SET(args, sqlContext, textFile)
  }

  /**
   * This method is used to save the followed node and the follower count using a RDD and the groupByKey method.
   *
   * @param sc
   * @param args
   */
  def RDD_G(sc: SparkContext, args: Array[String], textFile: RDD[String]) {
    val data = textFile.map(line => (line.split(",")(1), line.split(",")(0))).groupByKey()
    val followedData = data.map(d => (d._1, d._2.size))
    followedData.saveAsTextFile(args(1))
  }

  /**
   * This method is used to save the followed node and the follower count using a RDD and the reduceByKey method.
   *
   * @param sc
   * @param args
   */
  def RDD_R(sc: SparkContext, args: Array[String], textFile: RDD[String]) {
    val data = textFile.map(line => (line.split(",")(1), 1)).reduceByKey(_ + _)
    data.saveAsTextFile(args(1))
  }


  /**
   * This method is used to save the followed node and the follower count using a RDD and the foldByKey method.
   *
   * @param sc
   * @param args
   */
  def RDD_F(sc: SparkContext, args: Array[String], textFile: RDD[String]) {
    val data = textFile.map(line => (line.split(",")(1), 1)).foldByKey(0)(_ + _)
    data.saveAsTextFile(args(1))
  }


  /**
   * This method is used to save the followed node and the follower count using a RDD and the aggregateByKey method.
   *
   * @param sc
   * @param args
   */
  def RDD_A(sc: SparkContext, args: Array[String], textFile: RDD[String]) {
    val data = textFile.map(line => (line.split(",")(1), 1)).aggregateByKey(0)((acc, v) => acc + v, (v1, v2) => v1 + v2)
    data.saveAsTextFile(args(1))
  }


  /**
   * This method is used to save the the Followed node and the follower count using a DataSet and the groupBy method
   *
   * @param args
   * @param sqlContext
   * @param textFile
   */
  def D_SET(args: Array[String], sqlContext: SQLContext, textFile: RDD[String]) {
    import sqlContext.implicits._

    val myDF = textFile.map(line => (line.split(",")(0), line.split(",")(1))).toDF("Follower", "Followed")
    val myDS = myDF.select("Followed").map(Row => (Row.toString(), 1))
    myDS.groupBy("_1").count().coalesce(1).write.csv(args(1))
  }
}