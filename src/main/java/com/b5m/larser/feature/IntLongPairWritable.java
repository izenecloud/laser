package com.b5m.larser.feature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntLongPairWritable implements WritableComparable<IntLongPairWritable>{
	private final IntWritable INT_READER_WRITYER = new IntWritable();
	private final LongWritable LONG_READER_WRITYER = new LongWritable();
	
	private IntLongPair data;
	
	public IntLongPairWritable() {
		
	}
	
	public IntLongPairWritable(IntLongPair data) {
		this.data = data;
	}
	
	public IntLongPair get() {
		return data;
	}

	public void write(DataOutput out) throws IOException {
		INT_READER_WRITYER.set(data.getInt());
		INT_READER_WRITYER.write(out);
		
		LONG_READER_WRITYER.set(data.getLong());
		LONG_READER_WRITYER.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		INT_READER_WRITYER.readFields(in);
		LONG_READER_WRITYER.readFields(in);
		data = new IntLongPair(INT_READER_WRITYER.get(), LONG_READER_WRITYER.get());
	}

	public int compareTo(IntLongPairWritable other) {
		return data.compareTo(other.get());
	}

}
