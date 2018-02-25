package com.myBigData.spark;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.hbase.HBaseConfiguration;
        import org.apache.hadoop.hbase.KeyValue;
        import org.apache.hadoop.hbase.client.HTable;
        import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
        import org.apache.hadoop.hbase.mapred.TableOutputFormat;
        import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
        import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
        import org.apache.hadoop.hbase.util.Bytes;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.api.java.function.PairFunction;
        import scala.Tuple2;

public class SparkHbaseHfile {
    public static void main(String[] args) {
        try {
            System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
                    "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
            System.setProperty("javax.xml.parsers.SAXParserFactory",
                    "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
            //项目内部自己的配置类，可以忽略，其实就是设置sparkConf，然后获取到JavaSparkContext

            SparkConf sparkConf = new SparkConf().setAppName("dse load application in Java");
            sparkConf.setMaster("local");
            JavaSparkContext jsc = new JavaSparkContext(sparkConf);


            Configuration conf = HBaseConfiguration.create();
            String zk = "hdfsa,hdfsb,hdfsc";


            String tableName = "blog";
            conf.set("hbase.zookeeper.quorum", zk);
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            HTable table = new HTable(conf, tableName);
            conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
            Job job = Job.getInstance(conf);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);
            HFileOutputFormat.configureIncrementalLoad(job, table);
            String hdfsPath = "hdfs://vianet-hadoop-ha/tmp/data/blog.txt";
            JavaRDD<String> lines = jsc.textFile(hdfsPath);
            JavaPairRDD<ImmutableBytesWritable,KeyValue> hfileRdd = lines.mapToPair(new PairFunction<String, ImmutableBytesWritable, KeyValue>() {
                public Tuple2<ImmutableBytesWritable, KeyValue> call(String v1) throws Exception {
                    String[] tokens = v1.split(" ");
                    String rowkey = tokens[0];
                    String content = tokens[1];
                    KeyValue keyValue = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("article"), Bytes.toBytes("value"), Bytes.toBytes(content));
                    return new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue);
                }
            });
            String hfilePath = "hdfs://vianet-hadoop-ha/tmp/data/blog.hfile";
            hfileRdd.saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat.class, conf);

            //利用bulk load hfile
            LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(conf);
            bulkLoader.doBulkLoad(new Path(hfilePath), table);

        }catch(Exception e){
            e.printStackTrace();
        }finally {
            ;
        }
    }

}
