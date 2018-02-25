package com.myBigData.MapReduce.mapsidejoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin {

	static class MapSideJoinMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable> {
		Map<String, InfoBean> pdInfoMap = new HashMap<>();
		InfoBean k = new InfoBean();
		
		/**
         * setup方法是在maptask处理数据之前调用一次 可以用来做一些初始化工作
         */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("product.txt")));
			String line;
			while(StringUtils.isNotEmpty(line = br.readLine())) {
			//while (!(line = br.readLine()).isEmpty()) {	//報錯
				InfoBean pdBean = new InfoBean();
				String[] fields = line.split("\t");
				
				pdBean.set("", "", fields[0], -1, fields[1], fields[2], Float.parseFloat(fields[3]), "1");
				pdInfoMap.put(fields[0], pdBean);
			}
			br.close();
		}
		
		//
		// 由于已经持有完整的产品信息表，所以在map方法中就能实现join逻辑了
		//
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			InfoBean productBean = pdInfoMap.get(fields[2]);
			k.set(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), 
					productBean.getPname(), productBean.getCategory_id(), productBean.getPrice(), "0");
			
			context.write(k, NullWritable.get());
		}
	}
	
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MapSideJoin.class);
		
		job.setMapperClass(MapSideJoinMapper.class);
		
		job.setMapOutputKeyClass(InfoBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		// 指定需要缓存一个文件到所有的maptask运行节点工作目录
        /* job.addArchiveToClassPath(archive); */// 缓存jar包到task运行节点的classpath中
        /* job.addFileToClassPath(file); */// 缓存普通文件到task运行节点的classpath中
        /* job.addCacheArchive(uri); */// 缓存压缩包文件到task运行节点的工作目录
        /* job.addCacheFile(uri) */// 缓存普通文件到task运行节点的工作目录

		// 将产品表文件缓存到task工作节点的工作目录中去
		job.addCacheFile(new URI("hdfs://master/mr/mapsidejoin/cache/product.txt"));
		
		// map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);
        
        FileInputFormat.setInputPaths(job, new Path("hdfs://master/mr/mapsidejoin/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master/mr/mapsidejoin/output"));
        
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);	
	}
}
