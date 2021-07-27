package com.demo.hadoop.reducejoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Text k = new Text();

	private HashMap<String, String> pdMap = new HashMap<String, String>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		// 获取缓存文件
		try {
			URI[] cacheFiles = context.getCacheFiles();
			URI uri = new URI("hdfs://hadoop161:8020");
			String user = "root";
			FileSystem fs = FileSystem.get(uri, context.getConfiguration(), user);
			FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));
			// 读取流
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while ((line = reader.readLine()) != null) {
				// 产品主键	产品名称
				// 01,华为
				String[] split = line.split(",");
				// 将数据写入到map中
				pdMap.put(split[0], split[1]);
			}
			// 关闭流
			IOUtils.closeStream(reader);
		} catch (IOException | URISyntaxException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 订单主键	产品主键	数量
		// 001,01,1
		String line = value.toString();
		String[] split = line.split(",");
		// 获取产品名称
		String productName = pdMap.get(split[1]);
		
		// 封装输出结果
		// 订单主键	产品名称	数量
		// 001,华为,1
		k.set(split[0] + "," + productName + "," + split[1]);
		
		// 输出
		context.write(k, NullWritable.get());
	}

}
