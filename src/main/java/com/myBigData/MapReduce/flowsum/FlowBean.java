package com.myBigData.MapReduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {
	private long upFlow;
	private long dFlow;
	private long sumFlow;
	
	//如果空参构造函数被覆盖，一定要显示定义一下，否则在反序列时会抛异常
	public FlowBean() {}

	public FlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow + dFlow;
	}
	
	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	
	public void set(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow + dFlow;
	}

	@Override
	public String toString() {
		//return "FlowBean [upFlow=" + upFlow + ", dFlow=" + dFlow + ", sumFlow=" + sumFlow + "]";
		return upFlow + "\t" + dFlow + "\t" + sumFlow;
	}

	//反序列化，从输入流中读取各个字段信息
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.dFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	//序列化，将对象的字段信息写入输出流
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public int compareTo(FlowBean o) {
		//自定义倒序比较规则
		//从大到小, 当前对象和要比较的对象比, 如果当前对象大, 返回-1, 交换他们的位置(自己的理解)
		return this.sumFlow > o.getSumFlow() ? -1 : 1; 
	}

}
