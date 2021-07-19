package com.demo.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<LongWritable, Text, DataBean, Text> {

	DataBean k = new DataBean();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1 获取一行
		// mapper输入：索引值，文本 
		// key:0,value:  路口1	DataBean(east2westFlow=159, south2northFlow=99, west2eastFlow=219, north2southFlow=129, totalFlow=606)
		String line = value.toString();
		// 2 切割
		String[] words = line.split("\t");
		
		// 3 获取需要的字段
		String crossName = words[0]; // 路口
		String[] flowData = words[1].replace("(", "").replace(")", "").split(", "); // 方向数据
		
		long east2westFlow = Long.parseLong(flowData[0].split("=")[1]); // 东到西流量
		long south2northFlow = Long.parseLong(flowData[1].split("=")[1]); // 南到北流量
		long west2eastFlow = Long.parseLong(flowData[2].split("=")[1]); // 西到东流量
		long north2southFlow = Long.parseLong(flowData[3].split("=")[1]); // 北到南流量
		long totalFlow = east2westFlow + south2northFlow + west2eastFlow + north2southFlow; // 总合
		
		// 4 输出
		k.setEast2westFlow(east2westFlow);
		k.setNorth2southFlow(north2southFlow);
		k.setSouth2northFlow(south2northFlow);
		k.setWest2eastFlow(west2eastFlow);
		k.setTotalFlow(totalFlow);
		v.set(crossName);
		
		// mapper输出：DataBean(east2westFlow=159, south2northFlow=99, west2eastFlow=219, north2southFlow=129, totalFlow=606)，路口1
		context.write(k, v);
	}

}
