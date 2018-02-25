package com.myBigData.MapReduce.SecondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        configuration.addResource("core-site.xml");
        configuration.addResource("mapred-site.xml");
        configuration.addResource("yarn-site.xml");
        configuration.set("mapreduce.job.jar","D:\\myBigData\\target\\original-myBigData-1.0-SNAPSHOT.jar");


        Job job = Job.getInstance(configuration, "SecondarySort");
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySort");

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置map输出key value格式
        job.setMapOutputKeyClass(DateTemperaturePair.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置reduce输出key value格式
        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SecondarySortingMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: SecondarySortDriver"
                            + "<input-path> <output-path>");
        }
        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
        System.exit(returnStatus);
    }
}