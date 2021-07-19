package com.demo.hadoop.data;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, DataBean, Text, DataBean> {
	
	DataBean v = new DataBean();

	@Override
	protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
		// 1 累加求和
		// Reducer输入：文本，个数数组 
		// 路口1 :(DataBean,DataBean,DataBean)
		long east2westFlow = 0; // 东到西流量
		long south2northFlow = 0; // 南到北流量
		long west2eastFlow = 0; // 西到东流量
		long north2southFlow = 0; // 北到南流量
		long totalFlow = 0;
		for (DataBean item : values) {
			east2westFlow += item.getEast2westFlow();
			south2northFlow += item.getSouth2northFlow();
			west2eastFlow += item.getWest2eastFlow();
			north2southFlow += item.getNorth2southFlow();
			totalFlow += item.getTotalFlow(); // 累加每个行的total
		}
		
		// 2 输出
		// 路口1,DataBean
		v.setEast2westFlow(east2westFlow);
		v.setNorth2southFlow(north2southFlow);
		v.setSouth2northFlow(south2northFlow);
		v.setWest2eastFlow(west2eastFlow);
		v.setTotalFlow(totalFlow);
		
		context.write(key, v);
		System.out.println("reduce -> " + key.toString() + ":" + v.toString());
	}

}
