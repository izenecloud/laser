package com.b5m.admm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable implements Writable {

	private static final ArrayWritable writer = new ArrayWritable(
			DoubleWritable.class);

	private double[] arr;

	public DoubleArrayWritable() {
	}

	public DoubleArrayWritable(double[] arr) {
		this.arr = arr;
	}

	public void set(double[] arr) {
		this.arr = arr;
	}

	public void write(DataOutput out) throws IOException {
		Writable[] arr = new DoubleWritable[this.arr.length];
		for (int i = 0; i < this.arr.length; i++) {
			arr[i] = new DoubleWritable(this.arr[i]);
		}
		writer.set(arr);
		writer.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		writer.readFields(in);
		Writable[] arr = writer.get();
		this.arr = new double[arr.length];
		for (int i = 0; i < this.arr.length; i++) {
			this.arr[i] = ((DoubleWritable) arr[i]).get();
		}
	}

	public double[] get() {
		return arr;
	}

}
