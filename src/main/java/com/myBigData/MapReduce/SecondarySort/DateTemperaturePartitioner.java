package com.myBigData.MapReduce.SecondarySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair dataTemperaturePair, Text text,
                            int i) {
        return Math.abs(dataTemperaturePair.getYearMonth().hashCode() % i);
    }
}