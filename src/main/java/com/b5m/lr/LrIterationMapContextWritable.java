package com.b5m.lr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class LrIterationMapContextWritable implements Writable {
	private LrIterationMapContext context;

	public LrIterationMapContextWritable() {
		context = null;
	}

	public LrIterationMapContextWritable(LrIterationMapContext context) {
		this.context = context;
	}

	public LrIterationMapContext get() {
		return context;
	}

	public void readFields(DataInput in) throws IOException {
		ArrayWritable arr = new ArrayWritable(DoubleWritable.class);
		arr.readFields(in);
		DoubleWritable[] xWritable = (DoubleWritable[]) arr.toArray();
		double[] x = new double[xWritable.length];
		for (int i = 0; i < x.length; i++) {
			x[i] = xWritable[i].get();
		}
		context.setX(x);
	}

	public void write(DataOutput out) throws IOException {
		double[] x = context.getX();
		DoubleWritable[] xWritable = new DoubleWritable[x.length];
		for (int i = 0; i < x.length; i++) {
			xWritable[i] = new DoubleWritable(x[i]);
		}
		ArrayWritable arr = new ArrayWritable(DoubleWritable.class, xWritable);
		arr.write(out);
	}
}
