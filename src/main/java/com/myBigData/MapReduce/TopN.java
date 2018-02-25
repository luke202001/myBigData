package com.myBigData.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class TopN {
    public static class TopTenMapper extends  Mapper<Object, Text, NullWritable, IntWritable> {

        private SortedMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();
        private int N;

        public void map(Object key, Text value, Context context) {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                repToRecordMap.put(Integer.parseInt(itr.nextToken()), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
        }
        protected void setup(Context context) throws IOException,
                InterruptedException {
            this.N = context.getConfiguration().getInt("N", 10); // default is top 10
        }

        protected void cleanup(Context context) {
            for (Integer i : repToRecordMap.keySet()) {
                try {
                    context.write(NullWritable.get(), new IntWritable(i));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class TopTenReducer extends   Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        private int N;
        private SortedMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        public void reduce(NullWritable key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                repToRecordMap.put(value.get(), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
            for (Integer i : repToRecordMap.keySet()) {
                context.write(NullWritable.get(), new IntWritable(i));
            }
        }

        protected void setup(Mapper.Context context) throws IOException,
                InterruptedException {
            this.N = context.getConfiguration().getInt("N", 10); // default is top 10
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");
        conf.set("mapred.jar","D:\\mySpark\\target\\original-mySpark-1.0-SNAPSHOT.jar");

        Job job = Job.getInstance(conf);
        int N = Integer.parseInt("5"); // top N
        job.getConfiguration().setInt("N", 5);
        job.setJarByClass(TopN.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);// map阶段的输出的key
        job.setMapOutputValueClass(IntWritable.class);// map阶段的输出的value

        job.setOutputKeyClass(Text.class);// reduce阶段的输出的key
        job.setOutputValueClass(IntWritable.class);// reduce阶段的输出的value

        FileInputFormat.addInputPath(job, new Path("/test/topn/input"));
        FileOutputFormat.setOutputPath(job, new Path("/test/topn/output"));

        String outputPath =  "/test/topn/output";
        FileSystem fs = FileSystem.get(URI.create(outputPath), conf);
        Path path = new Path(outputPath);
        if (fs.exists(path)) {
            fs.deleteOnExit(path);
        }
        fs.close();

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
