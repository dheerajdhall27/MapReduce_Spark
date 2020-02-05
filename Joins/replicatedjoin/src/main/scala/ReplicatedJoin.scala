import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.broadcast

object ReplicatedJoin {

  case class TwitterData(Follower:Long, Followed: Long)

  def main(args: Array[String]) {
    var logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntf.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TriangleCount")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val maxFilter = 20000

    replicatedJoinUsingRDD(args, textFile, maxFilter)

//        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//        replicatedJoinUsingDataSet(args, sqlContext, textFile, maxFilter)
  }

  /**
   * This method is used to implement the replicated join using RDDs
   * @param args
   * @param textFile
   * @param maxFilter
   */
  def replicatedJoinUsingRDD(args: Array[String], textFile: RDD[String], maxFilter: Int) { //leftRDD: RDD[(String, String)], rightRDD: RDD[(String, String)], sc: SparkContext) = {

    val smallRDD = textFile.filter(data => (data.split(",")(0).toInt <= maxFilter && data.split(",")(1).toInt <= maxFilter))
      .map(line => (line.split(",")(0), line.split(",")(1)))
    val smallRDDLocal = smallRDD.groupByKey().mapValues(data => data).collectAsMap()
    val bigRDD = smallRDD

    bigRDD.sparkContext.broadcast(smallRDDLocal);

    val count = bigRDD.mapPartitions(iter => {
      iter.flatMap {
        case (key, value) => smallRDDLocal.get(value) match {
          case None => None
          case Some(v2) => v2.map(u => (u, key))

        }
      }
    }, preservesPartitioning = true).join(bigRDD).filter(data => (data._2._1.equals(data._2._2))).count()

    System.out.println("Triangle Count: " + count / 3)
    println("Triangle Count: " + count / 3)
  }


  /**
   * This method is used to implement the Replicated Join using DataFrame and DataSet
   * @param args
   * @param sqlContext
   * @param textFile
   * @param maxFilter
   */
  def replicatedJoinUsingDataSet(args: Array[String], sqlContext: SQLContext, textFile: RDD[String], maxFilter: Long) {
    import sqlContext.implicits._

//    val leftDF = textFile.map(line => (line.split(",")(0), line.split(",")(1))).filter(data => data._1.toLong <= maxFilter && data._2.toLong <= maxFilter)
//      .toDF("Follower", "FollowedA")
//    val rightDF = textFile.map(line => (line.split(",")(1), line.split(",")(0))).filter(data => data._1.toInt <= maxFilter && data._2.toInt <= maxFilter)
//      .toDF("Follower", "FollowedB")

    val leftDS = textFile.filter(data => data.split(",")(0).toInt <= maxFilter && data.split(",")(1).toInt <= maxFilter)
      .map(line => TwitterData(line.split(",")(0).toLong, line.split(",")(1).toLong)).toDS()
    val rightDS = textFile.filter(data => data.split(",")(0).toInt <= maxFilter && data.split(",")(1).toInt <= maxFilter)
      .map(line => TwitterData(line.split(",")(1).toLong, line.split(",")(0).toLong)).toDS()

    val joinedDS = leftDS.join(broadcast(rightDS), "Follower").map(data => TwitterData(data.getLong(1), data.getLong(2)))
    joinedDS.explain()

    val count = joinedDS.join(leftDS, "Follower").filter(data => data.getLong(1) == data.getLong(2)).count()
        System.out.println("Triangle Count: " + count/ 3)
        println("Triangle Count: " + count/ 3)

//    val df =  leftDF.join(broadcast(rightDF), "Follower").map(data => (data.getString(1), data.getString(2))).toDF("Follower", "FollowedC")
//
//    val count = leftDF.join(df, "Follower").filter(data => (data.getString(1).equals(data.getString(2)))).count()
//
  }
}
