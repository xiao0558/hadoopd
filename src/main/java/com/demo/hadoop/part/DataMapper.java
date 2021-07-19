package com.demo.hadoop.part;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<LongWritable, Text, Text, DataBean> {

	Text k = new Text();
	DataBean v = new DataBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1 获取一行
		// mapper输入：索引值，文本 
		// key:0,value:1,路口1,51,31,71,41,2021-06-24 01:00:00
		String line = value.toString();
		// 2 切割
		String[] words = line.split(",");
		
		// 3 获取需要的字段
		String crossName = words[1]; // 路口
		long east2westFlow = Long.parseLong(words[2]); // 东到西流量
		long south2northFlow = Long.parseLong(words[3]); // 南到北流量
		long west2eastFlow = Long.parseLong(words[4]); // 西到东流量
		long north2southFlow = Long.parseLong(words[5]); // 北到南流量
		long totalFlow = east2westFlow + south2northFlow + west2eastFlow + north2southFlow; // 总合
				
		// 4 输出
		k.set(crossName);
		v.setEast2westFlow(east2westFlow);
		v.setNorth2southFlow(north2southFlow);
		v.setSouth2northFlow(south2northFlow);
		v.setWest2eastFlow(west2eastFlow);
		v.setTotalFlow(totalFlow);
		
		// mapper输出：路口1，51,31,71,41
		context.write(k, v);
	}

}
