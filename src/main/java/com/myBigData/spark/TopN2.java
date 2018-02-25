package com.myBigData.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;


public class TopN2 {
    public static void main(String[] args) throws Exception {
        // 输入处理参数
        if (args.length < 2) {
            System.err.println("Usage: Top10 <input-path> <topN>");
            System.exit(1);
        }
        System.out.println("args[0]: <input-path>="+args[0]);
        System.out.println("args[1]: <topN>="+args[1]);
        final int N = Integer.parseInt(args[1]);

        // 创建一个javaSpark上下文对象
        SparkConf sparkConf = new SparkConf().setAppName("TopN").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        int topN = 10;


        // 将TOP N 广播到所有集群节点
        final Broadcast<Integer> broadcastTopN = ctx.broadcast(topN);
        // now topN is available to be read from all cluster nodes

        // 创建第一个RDD，格式是这样的A,2 | B,2 |C,3这样
        //<string-key><,><integer-value-count>
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        lines.saveAsTextFile("/output/1");

        // RDD分区，返回一个新的RDD，归约到numPartitions分区
        //分区的原则：每个执行器使用（2*num_executors*cores_per_executor）个分区
        JavaRDD<String> rdd = lines.coalesce(9);

        // 将输入（T）映射到（K,V）对
        // PairFunction<T, K, V>
        // T => Tuple2<K, V>
        JavaPairRDD<String,Integer> kv = rdd.mapToPair(new PairFunction<String,String,Integer>() {
            @Override
            public Tuple2<String,Integer> call(String s) {
                String[] tokens = s.split(","); // url,789
                return new Tuple2<String,Integer>(tokens[0], Integer.parseInt(tokens[0]));
            }
        });
        kv.saveAsTextFile("/output/2");

        //用Function函数对重复键进行归约
        JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        uniqueKeys.saveAsTextFile("/output/3");

        // 为本地的partitions创建本地的TOP N
        JavaRDD<SortedMap<Integer, String>> partitions = uniqueKeys.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<String,Integer>>, SortedMap<Integer, String>>() {
                    @Override
                    public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String,Integer>> iter) {
                        final int N = broadcastTopN.value();
                        SortedMap<Integer, String> localTopN = new TreeMap<Integer, String>();
                        while (iter.hasNext()) {
                            Tuple2<String,Integer> tuple = iter.next();
                            localTopN.put(tuple._2, tuple._1);
                            // keep only top N
                            if (localTopN.size() > N) {
                                localTopN.remove(localTopN.firstKey());
                            }
                        }
                        return Collections.singletonList(localTopN).iterator();
                    }
                });
        partitions.saveAsTextFile("/output/4");

        // 获得最终的TOP N
        SortedMap<Integer, String> finalTopN = new TreeMap<Integer, String>();
        //获得所有分区的TOP N
        List<SortedMap<Integer, String>> allTopN = partitions.collect();
        for (SortedMap<Integer, String> localTopN : allTopN) {
            for (Map.Entry<Integer, String> entry : localTopN.entrySet()) {
                // count = entry.getKey()
                // url = entry.getValue()
                finalTopN.put(entry.getKey(), entry.getValue());
                // keep only top N
                if (finalTopN.size() > N) {
                    finalTopN.remove(finalTopN.firstKey());
                }
            }
        }

        //输出最终的TOP N
        for (Map.Entry<Integer, String> entry : finalTopN.entrySet()) {
            System.out.println(entry.getKey() + "------" + entry.getValue());
        }

        System.exit(0);
    }
}
