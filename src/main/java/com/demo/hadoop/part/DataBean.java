package com.demo.hadoop.part;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Data;

@Data
public class DataBean implements Writable {

	private long east2westFlow; // 东到西流量
	private long south2northFlow; // 南到北流量
	private long west2eastFlow; // 西到东流量
	private long north2southFlow; // 北到南流量
	private long totalFlow;
	
	// 序列化
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(east2westFlow); // 序列化顺序位置0
		out.writeLong(south2northFlow); // 序列化顺序位置1
		out.writeLong(west2eastFlow); // 序列化顺序位置2
		out.writeLong(north2southFlow); // 序列化顺序位置3
		out.writeLong(totalFlow); // 序列化顺序位置4
	}

	// 反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		east2westFlow = in.readLong(); // 反序列化顺序位置0
		south2northFlow = in.readLong(); // 反序列化顺序位置1
		west2eastFlow = in.readLong(); // 反序列化顺序位置2
		north2southFlow = in.readLong(); // 反序列化顺序位置3
		totalFlow = in.readLong(); // 反序列化顺序位置4
	}

}
