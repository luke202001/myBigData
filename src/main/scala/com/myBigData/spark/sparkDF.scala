package com.myBigData.spark

import org.apache.spark.sql.SparkSession

case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)
object sparkDF {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
        .master(
          "local"
        )
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .config("spark.yarn.jars","hdfs://vianet-hadoop-ha/tmp/sparkjars/*")
       // .config("spark.local.dir","e://tmp")


      .getOrCreate()

    //spark.sparkContext.addJar("D:\\mySpark\\target\\original-mySpark-1.0-SNAPSHOT.jar")


    import spark.implicits._
    val bankText = spark.sparkContext.textFile("hdfs://vianet-hadoop-ha/tmp/data/bank.csv")
      .map(_.split(","))
      .map( s => Bank(s(0).toInt,   s(1),  s(2),   s(3),  s(5).toInt ) )
      //.take(10).foreach(println)
    //  .foreach(println)
      .toDF()
     bankText.show()


}}
