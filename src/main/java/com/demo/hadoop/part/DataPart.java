package com.demo.hadoop.part;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DataPart extends Partitioner<Text, DataBean> {

	@Override
	public int getPartition(Text key, DataBean value, int numPartitions) {
		String text = key.toString();
		if ("路口1".equals(text)) {
			numPartitions = 0;
		} else if ("路口2".equals(text)) {
			numPartitions = 1;
		} else if ("路口3".equals(text)) {
			numPartitions = 2;
		}
		return numPartitions;
	}

}
