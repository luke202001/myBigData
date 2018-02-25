package com.myBigData.hbaseETL.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.classification.InterfaceAudience
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

@InterfaceAudience.Public
class NewHBaseRDD[K,V](@transient sc : SparkContext,
                       @transient inputFormatClass: Class[_ <: InputFormat[K, V]],
                       @transient keyClass: Class[K],
                       @transient valueClass: Class[V],
                   @transient conf: Configuration,
                   val hBaseContext: HBaseContext) extends NewHadoopRDD(sc,inputFormatClass, keyClass, valueClass, conf) {

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    hBaseContext.applyCreds()
    super.compute(theSplit, context)
  }
}
