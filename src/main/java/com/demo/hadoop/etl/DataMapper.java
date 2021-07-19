package com.demo.hadoop.etl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 输入数据
		// 2021-07-01 11:13:38,276 ERROR org.apache.hadoop.hdfs.server.namenode.NameNode: RECEIVED SIGNAL 1: SIGHUP
		String line = value.toString();
		String[] split = line.split(" ");
		// 日志级别
		if (split.length < 3) { // 
			// 切割后的数量，小于3个，这样的数据忽略
			return;
		}
		String level = split[2]; // 第三个位置
		if (level.equals("WARN") || level.equals("ERROR")) {
			k.set(line);
			// 输出
			context.write(k, NullWritable.get());
		} else {
			// 不满足要求的数据就跳过了
			return;
		}
	}

}
