package com.demo.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<DataBean, Text, Text, DataBean> {

	@Override
	protected void reduce(DataBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Reducer输入：DataBean(east2westFlow=159, south2northFlow=99, west2eastFlow=219, north2southFlow=129, totalFlow=606)，路口1
		for (Text value : values) {
			// 2 输出：路口1,DataBean(east2westFlow=159, south2northFlow=99, west2eastFlow=219, north2southFlow=129, totalFlow=606)
			context.write(value, key);
		}
	}

}
