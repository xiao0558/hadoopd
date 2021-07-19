package com.demo.hadoop.part;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// 1 获取配置信息以及获取 job 对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 2 关联本 Driver 程序的 jar
		job.setJarByClass(DataDriver.class);
		
		// 3 关联 Mapper 和 Reducer 的 jar
		job.setMapperClass(DataMapper.class);
		job.setReducerClass(DataReducer.class);
		
		// 4 设置 Mapper 输出的 kv 类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataBean.class);
		
		// 5 设置Reducer输出 kv 类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		
		// 配置自定义分区类
		job.setPartitionerClass(DataPart.class);
		job.setNumReduceTasks(3); // 必须与自定义分区类中的数量一致
		
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop161:8020/hadoopc_input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop161:8020/hadoopc_output"));
		
		// 7 提交 job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
	
}
