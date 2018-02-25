package com.myBigData.spark

import org.apache.spark.rdd.RDD.{rddToOrderedRDDFunctions, rddToPairRDDFunctions}
import org.apache.spark.{SparkConf, SparkContext}

object SecondarySort {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(" Secondary Sort ")
      .setMaster("local")
    var sc = new SparkContext(conf)
    sc.setLogLevel("Warn")
    val file = sc.textFile("/test/temp.txt")
    //val file = sc.textFile("e:\\SecondarySort.txt")
    val rdd = file.map(line => line.split(","))
      .map(x => ((x(0), x(1)), x(3))).groupByKey().sortByKey(false)
      .map(x => (x._1._1 + "-" + x._1._2, x._2.toList.sortWith(_ > _)))
    rdd.foreach(
      x => {
        val buf = new StringBuilder()
        for (a <- x._2) {
          buf.append(a)
          buf.append(",")
        }
        buf.deleteCharAt(buf.length() - 1)
        println(x._1 + " " + buf.toString())
      })
    sc.stop()
  }
}
