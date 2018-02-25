package com.myBigData.MapReduce.provinceflow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 定义自己的从map到reduce之间的数据（分组）分发规则 按照手机号所属的省份来分发（分组）ProvincePartitioner
 * 默认的分组组件是HashPartitioner
 * @author: yangzheng
 */

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

	private static HashMap<String, Integer> provinceDict = new HashMap<String, Integer>();
	
	static {
		provinceDict.put("136", 0);
		provinceDict.put("137", 1);
		provinceDict.put("138", 2);
		provinceDict.put("139", 3);
	}
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String prefix = key.toString().substring(0, 3);
		Integer provinceId = provinceDict.get(prefix);
		
		return provinceId == null ?  4 : provinceId;
	}
}
