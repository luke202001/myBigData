package com.myBigData.MapReduce.provinceflow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCount {

	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		private Text k = new Text();
		private FlowBean v = new FlowBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			String phoneStr = fields[1];
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long dFlow = Long.parseLong(fields[fields.length - 2]);
			
			k.set(phoneStr);
			v.set(upFlow, dFlow);
			
			context.write(k, v);
		}
	}
	
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		private FlowBean v = new FlowBean();
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
			long upFlow = 0;
			long dFlow = 0;
			for(FlowBean value : values) {
				upFlow += value.getUpFlow();
				dFlow += value.getdFlow();
			}
			
			v.set(upFlow, dFlow);
			
			context.write(key, v);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.addResource("core-site.xml");
		//conf.addResource("mapred-site.xml");
		//conf.addResource("yarn-site.xml");
		conf.set("mapreduce.jobtracker.address","local");
		conf.set("mapreduce.framework.name", "local");
		conf.set("mapreduce.cluster.local.dir", "D:\\myBigData\\target\\");
		conf.set("mapreduce.job.jar", "D:\\myBigData\\target\\original-myBigData-1.0-SNAPSHOT.jar");

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://vianet-hadoop-ha/test/provinceflow/input/data.dat")); //设置map输入文件路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://vianet-hadoop-ha/test/provinceflow/output")); //设置reduce输出文件路径
		// 准备清理已存在的输出目录
		Path outputPath = new Path("hdfs://vianet-hadoop-ha/test/provinceflow/output");
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath, true);
			System.out.println("output file exists, but is has deleted");
		}
		//指定我们自定义的数据分区器
		job.setPartitionerClass(ProvincePartitioner.class);
		//同时指定相应“分区”数量的reducetask
		job.setNumReduceTasks(5);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
