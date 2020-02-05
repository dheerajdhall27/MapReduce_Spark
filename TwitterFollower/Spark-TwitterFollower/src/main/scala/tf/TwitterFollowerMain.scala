package tf

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object TwitterFollowerMain {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntf.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val counts = textFile.map(line => (line.split(",")(1), 1))
        .reduceByKey(_+_)

    logger.info(counts.toDebugString)
    counts.saveAsTextFile(args(1))
  }
}
