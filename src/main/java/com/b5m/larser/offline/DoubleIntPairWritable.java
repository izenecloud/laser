package com.b5m.larser.offline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DoubleIntPairWritable implements
		WritableComparable<DoubleIntPairWritable> {

	private int key;
	private double value;

	public DoubleIntPairWritable() {
	}

	public DoubleIntPairWritable(int k, double v) {
		this.key = k;
		this.value = v;
	}

	public void setKey(int k) {
		this.key = k;
	}

	public void setValue(double v) {
		this.value = v;
	}

	public void readFields(DataInput in) throws IOException {
		this.key = in.readInt();
		this.value = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(key);
		out.writeDouble(value);
	}

	public int getKey() {
		return key;
	}

	public double getValue() {
		return value;
	}

	public int compareTo(DoubleIntPairWritable o) {
		if (this.value > o.value) {
			return 1;
		} else if (this.value < o.value) {
			return -1;
		}
		return 0;
	}
}
