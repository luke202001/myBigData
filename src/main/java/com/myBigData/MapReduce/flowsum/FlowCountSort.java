package com.myBigData.MapReduce.flowsum;

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

/*
13480253104     180     180     360
13502468823     7335    110349  117684
13560436666     1116    954     2070
13560439658     2034    5892    7926
13602846565     1938    2910    4848
13660577991     6960    690     7650
13719199419     240     0       240
13726230503     2481    24681   27162
13726238888     2481    24681   27162
13760778710     120     120     240
13826544101     264     0       264
13922314466     3008    3720    6728
13925057413     11058   48243   59301
13926251106     240     0       240
13926435656     132     1512    1644
15013685858     3659    3538    7197
15920133257     3156    2936    6092
15989002119     1938    180     2118
18211575961     1527    2106    3633
18320173382     9531    2412    11943
84138413        4116    1432    5548
 */

public class FlowCountSort {

	static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
		private Text v = new Text();
		private FlowBean k = new FlowBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			String phoneStr = fields[0];
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long dFlow = Long.parseLong(fields[fields.length - 2]);
			v.set(phoneStr);
			k.set(upFlow, dFlow);
			
			context.write(k, v);
		}
		
	}
	
	static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(values.iterator().next(), bean);
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
		
		job.setJarByClass(FlowCountSort.class);
		
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://vianet-hadoop-ha/test/flowsum/input/data.dat")); //设置map输入文件路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://vianet-hadoop-ha/test/flowsum/output")); //设置reduce输出文件路径
		// 准备清理已存在的输出目录
		Path outputPath = new Path("hdfs://vianet-hadoop-ha/test/flowsum/output");
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath, true);
			System.out.println("output file exists, but is has deleted");
		}

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
		
	}
}
