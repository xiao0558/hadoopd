package com.demo.hadoop.mapperjoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, DataBean, DataBean, NullWritable> {
	
	DataBean v = new DataBean();

	@Override
	protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
//		输入数据组
//		01	001	1		order	reduce
//		01	004	4		order	
//		01			华为	product	
		// 订单集合
		List<DataBean> orderList = new ArrayList<DataBean>();
		// 产品对象
		DataBean product = new DataBean();
		
		// 遍历输入数据,values迭代器需要重新赋值到对象，不然对象会被覆盖
		for (DataBean item : values) {
			if (item.getTableFlag().equals("order")) { // 订单数据
				DataBean order = new DataBean();
				try {
					BeanUtils.copyProperties(order, item);
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
				orderList.add(order);
			} else { // 产品数据
				try {
					BeanUtils.copyProperties(product, item);
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
		
		// 给集合赋值产品名称
		for (DataBean dataBean : orderList) {
			dataBean.setProductName(product.getProductName());
			// 输出
			context.write(dataBean, NullWritable.get());
		}
	}

}
