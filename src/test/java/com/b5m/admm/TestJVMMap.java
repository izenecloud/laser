package com.b5m.admm;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class TestJVMMap extends Mapper<Writable, Writable, Writable, Writable> {
	private double[] data;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		// System.out.println(1024 * 1024 * 1024);
		data = new double[(int) (1024 * 1024 * 1024 * 2 / 8)];
	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		cleanup(context);
	}
}
