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

/**
对日志数据中的上下行流量信息汇总，并输出按照总流量倒序排序的结果
数据如下：
data.dat
1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
1363157995052 	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4			4	0	264	0	200
1363157991076 	13926435656	20-10-7A-28-CC-0A:CMCC	120.196.100.99			2	4	132	1512	200
1363154400022 	13926251106	5C-0E-8B-8B-B1-50:CMCC	120.197.40.4			4	0	240	0	200
1363157993044 	18211575961	94-71-AC-CD-E6-18:CMCC-EASY	120.196.100.99	iface.qiyi.com	视频网站	15	12	1527	2106	200
1363157995074 	84138413	5C-0E-8B-8C-E8-20:7DaysInn	120.197.40.4	122.72.52.12		20	16	4116	1432	200
1363157993055 	13560439658	C4-17-FE-BA-DE-D9:CMCC	120.196.100.99			18	15	1116	954	200
1363157995033 	15920133257	5C-0E-8B-C7-BA-20:CMCC	120.197.40.4	sug.so.360.cn	信息安全	20	20	3156	2936	200
1363157983019	13719199419	68-A1-B7-03-07-B1:CMCC-EASY	120.196.100.82			4	0	240	0	200
1363157984041 	13660577991	5C-0E-8B-92-5C-20:CMCC-EASY	120.197.40.4	s19.cnzz.com	站点统计	24	9	6960	690	200
1363157973098 	15013685858	5C-0E-8B-C7-F7-90:CMCC	120.197.40.4	rank.ie.sogou.com	搜索引擎	28	27	3659	3538	200
1363157986029 	15989002119	E8-99-C4-4E-93-E0:CMCC-EASY	120.196.100.99	www.umeng.com	站点统计	3	3	1938	180	200
1363157992093 	13560439658	C4-17-FE-BA-DE-D9:CMCC	120.196.100.99			15	9	918	4938	200
1363157986041 	13480253104	5C-0E-8B-C7-FC-80:CMCC-EASY	120.197.40.4			3	3	180	180	200
1363157984040 	13602846565	5C-0E-8B-8B-B6-00:CMCC	120.197.40.4	2052.flash2-http.qq.com	综合门户	15	12	1938	2910	200
1363157995093 	13922314466	00-FD-07-A2-EC-BA:CMCC	120.196.100.82	img.qfc.cn		12	12	3008	3720	200
1363157982040 	13502468823	5C-0A-5B-6A-0B-D4:CMCC-EASY	120.196.100.99	y0.ifengimg.com	综合门户	57	102	7335	110349	200
1363157986072 	18320173382	84-25-DB-4F-10-1A:CMCC-EASY	120.196.100.99	input.shouji.sogou.com	搜索引擎	21	18	9531	2412	200
1363157990043 	13925057413	00-1F-64-E1-E6-9A:CMCC	120.196.100.55	t3.baidu.com	搜索引擎	69	63	11058	48243	200
1363157988072 	13760778710	00-FD-07-A4-7B-08:CMCC	120.196.100.82			2	2	120	120	200
1363157985066 	13726238888	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
1363157993055 	13560436666	C4-17-FE-BA-DE-D9:CMCC	120.196.100.99			18	15	1116	954	200
 */
public class FlowCount {

	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		private Text k = new Text();
		private FlowBean v = new FlowBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String phoneString = fields[1];
			long upFlow = Long.parseLong(fields[fields.length - 3]); 
			long dFlow = Long.parseLong(fields[fields.length - 2]);
			
			k.set(phoneString);
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
			for (FlowBean value : values) {
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
