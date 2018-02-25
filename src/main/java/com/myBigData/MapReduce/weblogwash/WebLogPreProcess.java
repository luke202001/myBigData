package com.myBigData.MapReduce.weblogwash;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 处理原始日志，过滤出真实pv请求
 * 转换时间格式
 * 对缺失字段填充默认值
 * 对记录标记valid和invalid
 * 
 * @author
 *
 */

public class WebLogPreProcess {

	static class WebLogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		//用来存储网站url分类数据
		Set<String> pages = new HashSet<String>();
		private Text k = new Text();
		private NullWritable v = NullWritable.get();
		
		/**
		 * 从外部加载网站url分类数据
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			pages.add("/about");
			pages.add("/black-ip-list/");
			pages.add("/cassandra-clustor/");
			pages.add("/finance-rhive-repurchase/");
			pages.add("/hadoop-family-roadmap/");
			pages.add("/hadoop-hive-intro/");
			pages.add("/hadoop-zookeeper-intro/");
			pages.add("/hadoop-mahout-roadmap/");
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			WebLogBean bean = WebLogParser.parser(line);
			
			// 过滤js/图片/css等静态资源 
			WebLogParser.filtStaticResource(bean, pages);
			
			/*if (!bean.isValid()) { return; }*/
			
			k.set(bean.toString());
			context.write(k, v);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//conf.addResource("core-site.xml");
		//conf.addResource("mapred-site.xml");
		//conf.addResource("yarn-site.xml");
		conf.set("mapreduce.jobtracker.address","local");
		conf.set("mapreduce.framework.name", "local");
		conf.set("mapreduce.cluster.local.dir", "D:\\myBigData\\target\\");
		conf.set("mapreduce.job.jar", "D:\\myBigData\\target\\original-myBigData-1.0-SNAPSHOT.jar");

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WebLogPreProcess.class);
		
		job.setMapperClass(WebLogPreProcessMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://vianet-hadoop-ha/test/weblogwash/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://vianet-hadoop-ha/test/weblogwash/output"));
		// 准备清理已存在的输出目录
		Path outputPath = new Path("hdfs://vianet-hadoop-ha/test/weblogwash/output");
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath, true);
			System.out.println("output file exists, but is has deleted");
		}
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
