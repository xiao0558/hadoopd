package com.demo.hadoop.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Data;

@Data
public class DataBean implements Writable {

	private String orderId; // 订单主键
	private String productId; // 产品主键
	private int amount; // 数量
	private String productName; // 产品名称
	private String tableFlag; // 表名标记
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeUTF(productId);
		out.writeInt(amount);
		out.writeUTF(productName);
		out.writeUTF(tableFlag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.productId = in.readUTF();
		this.amount = in.readInt();
		this.productName = in.readUTF();
		this.tableFlag = in.readUTF();
	}

	@Override
	public String toString() {
		//订单主键	产品名称	数量
		//001,华为,1
		return orderId + "," + productName + "," + amount;
	}
	
}
