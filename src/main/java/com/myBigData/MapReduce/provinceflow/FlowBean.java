package com.myBigData.MapReduce.provinceflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {
	private long upFlow;
	private long dFlow;
	private long sumFlow;
	
	public FlowBean() {}

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
		this.sumFlow = upFlow +  dFlow;
	}


	@Override
	public String toString() {
		//return "FlowBean [upFlow=" + upFlow + ", dFlow=" + dFlow + ", sumFlow=" + sumFlow + "]";
		return upFlow + "\t" + dFlow + "\t" + sumFlow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.dFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public int compareTo(FlowBean o) {
		return this.sumFlow > o.getSumFlow() ? -1 : 1;
	}
}
