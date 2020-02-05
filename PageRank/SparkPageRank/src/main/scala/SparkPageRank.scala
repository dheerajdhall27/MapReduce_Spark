import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
* This Spark Object represents the page rank calculation for a synthetic graph
*/
object SparkPageRank {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("SparkPageRank").master("local[4]").getOrCreate()

    val iters = 10
    val k = 100

    val lines = test(k, spark)
    val links = lines.map(s => {
      (s._1, s._2)
    }).distinct().groupByKey().cache()

    val totalSize = k * k

    var ranks = links.map(data => {
      if (data._1 == 0) {
        (data._1, 0.0)
      } else {
        (data._1, 1.0 / totalSize)
      }
    })


    // Calculates the page rank and displays the lineage
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).map(data => {
        if (data._1 != 0) {
          (data._1, (0.15 / totalSize) + 0.85 * data._2)
        } else {
          (data._1, 0.85 * data._2)
        }
      })
      ranks = links.leftOuterJoin(ranks).mapValues {
        case (d, Some(v)) => v
        case (d, None) => 0.15 / totalSize
      }

      val zeroRank = ranks.lookup(0)
      val accumulatedRank = zeroRank.head / totalSize

      //      ranks = ranks.mapValues(v => v + accumulatedRank)
      ranks = ranks.map(data => {
        if (data._1 == 0) {
          (data._1, 0)
        } else {
          (data._1, data._2 + accumulatedRank)
        }
      })
      ranks.collect()

      if (i <= 3) {
        println("Iteration: " + i);
        println(ranks.toDebugString)
      }
    }

    val output = spark.sparkContext.union(ranks)
    output.filter(data => {
      (data._1 <= 100)
    }).saveAsTextFile(args(1))

    spark.stop()
  }

  // This method is used to create the synthetic graph
  def test(k: Int, sparkSession: SparkSession): RDD[(Int, Int)] = {

    val test = List.range(1, k * k)

    val testMap = scala.collection.mutable.Map[Int, Int]()
    for (i <- 1 to test.size + 1) {
      if (i % k == 0) {
        testMap(i) = 0
      } else {
        testMap(i) = (i + 1)
      }
    }

    testMap(0) = Int.MinValue

    val rdd = sparkSession.sparkContext.parallelize(testMap.toList)
    rdd
  }
}

