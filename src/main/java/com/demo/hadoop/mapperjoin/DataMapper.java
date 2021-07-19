package com.demo.hadoop.mapperjoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DataMapper extends Mapper<LongWritable, Text, Text, DataBean> {

	private String fileName;

	Text k = new Text();
	DataBean v = new DataBean();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DataBean>.Context context) throws IOException, InterruptedException {
		FileSplit fs = (FileSplit) context.getInputSplit();
		// 获取文件名
		fileName = fs.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 读取一行数据
		String line = value.toString();
		if (fileName.equals("order.txt")) {
			// 处理订单逻辑
			// 订单主键	产品主键	数量
			// 001,01,1
			String[] split = line.split(",");
			k.set(split[1]); // key:产品主键
			v.setOrderId(split[0]);
			v.setProductId(split[1]);
			v.setAmount(Integer.parseInt(split[2]));
			v.setProductName(""); // order数据没有这个字段
			v.setTableFlag("order");
			// 输出结构
			// 01,001,1,order
		} else {
			// 处理商品逻辑
			// 产品主键	产品名称
			// 01,华为
			String[] split = line.split(",");
			k.set(split[0]); // key:产品主键
			v.setOrderId(""); // product数据没有这个字段
			v.setProductId(split[0]);
			v.setAmount(0); // product数据没有这个字段
			v.setProductName(split[1]);
			v.setTableFlag("product");
			// 输出结构
			// 01,华为,product
		}
		// mapper输出
		context.write(k, v);
	}

}
