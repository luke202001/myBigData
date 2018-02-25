package com.myBigData.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class TopN {

    public static void main(String[] args) throws Exception {

        // 输入处理参数
        if (args.length < 1) {
            System.err.println("Usage: Top10 <input-file>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("args[0]: <input-path>="+inputPath);

        // 连接到spark master

        SparkConf sparkConf = new SparkConf().setAppName("TopN").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        int topN = 10;
        final Broadcast<Integer> broadcastTopN = ctx.broadcast(topN);
        // 从HDFS中读取文件并创建第一个RDD
        //  <string-key><,><integer-value>,
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);


        // 从现有的JavaRDD<String>创建一个新的成对的RDDJavaPairRDD<String,Integer>
        // Spark Java类：PairFunction<T, K, V>
        // 函数类型：T => Tuple2<K, V>
        //其实每一个JavaPairRDD<String,Integer>也即是Tuple2<String,Integer>()
        JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(String s) {
                String[] tokens = s.split(","); // cat7,234
                return new Tuple2<String,Integer>(tokens[0], Integer.parseInt(tokens[0]));
            }
        });

        List<Tuple2<String,Integer>> debug1 = pairs.collect();
        for (Tuple2<String,Integer> t2 : debug1) {
            System.out.println("key="+t2._1 + "\t value= " + t2._2);
        }


        // 为各个输入分区创建一个本地TOP 10列表
        JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<String,Integer>>, SortedMap<Integer, String>>() {
                    @Override
                    public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String,Integer>> iter) {
                        //setup();
                        SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();
                        while (iter.hasNext()) {
                            Tuple2<String,Integer> tuple = iter.next();
                            top10.put(tuple._2, tuple._1);
                            // keep only top N
                            if (top10.size() >  broadcastTopN.value()) {
                                top10.remove(top10.firstKey());
                            }
                        }
                       // cleanUp();
                        //singletonList确保唯一性
                        return Collections.singletonList(top10).iterator();
                    }
                });


        SortedMap<Integer, String> finaltop10 = new TreeMap<Integer, String>();
        //使用collect得到所有TOP 10列表
        List<SortedMap<Integer, String>> alltop10 = partitions.collect();
        //获得最终所有的TOP 10
        for (SortedMap<Integer, String> localtop10 : alltop10) {
            //System.out.println(tuple._1 + ": " + tuple._2);
            // weight/count = tuple._1
            // catname/URL = tuple._2
            for (Map.Entry<Integer, String> entry : localtop10.entrySet()) {
                //   System.out.println(entry.getKey() + "--" + entry.getValue());
                finaltop10.put(entry.getKey(), entry.getValue());
                // keep only top 10
                if (finaltop10.size() >  broadcastTopN.value()) {
                    finaltop10.remove(finaltop10.firstKey());
                }
            }
        }

        // 输出最终的TOP 10列表
        for (Map.Entry<Integer, String> entry : finaltop10.entrySet()) {
            System.out.println(entry.getKey() + "------" + entry.getValue());
        }

        System.exit(0);
    }


}
