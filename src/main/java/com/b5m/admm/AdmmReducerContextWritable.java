package com.b5m.admm;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdmmReducerContextWritable implements Writable {
	private AdmmReducerContext context;
	private DoubleArrayWritable writer = new DoubleArrayWritable();

	public AdmmReducerContextWritable(AdmmReducerContext context) {
		this.context = context;
	}

	public AdmmReducerContextWritable() {
	}

	public AdmmReducerContext get() {
		return context;
	}

	public void readFields(DataInput in) throws IOException {
		BooleanWritable flag = new BooleanWritable(false);
		flag.readFields(in);
		double[] xUpdated = null;
		if (flag.get()) {
			DoubleArrayWritable reader = new DoubleArrayWritable();
			reader.readFields(in);
			xUpdated = reader.get();
		}
		flag.set(false);

		double[] uInitial = null;
		flag.readFields(in);
		if (flag.get()) {
			DoubleArrayWritable reader = new DoubleArrayWritable();
			reader.readFields(in);
			uInitial = reader.get();
		}
		flag.set(false);

		double[] zUpdated = null;
		flag.readFields(in);
		if (flag.get()) {
			DoubleArrayWritable reader = new DoubleArrayWritable();
			reader.readFields(in);
			zUpdated = reader.get();
		}
		flag.set(false);

		DoubleWritable rho = new DoubleWritable();
		rho.readFields(in);

		DoubleWritable lambda = new DoubleWritable();
		lambda.readFields(in);

		DoubleWritable primalObjectiveValue = new DoubleWritable();
		primalObjectiveValue.readFields(in);

		LongWritable count = new LongWritable();
		count.readFields(in);

		context = new AdmmReducerContext(null, uInitial, xUpdated, zUpdated,
				primalObjectiveValue.get(), rho.get(), lambda.get(),
				count.get());
	}

	public void write(DataOutput out) throws IOException {
		new BooleanWritable(null != context.getXUpdated()).write(out);
		if (null != context.getXUpdated()) {
			writer.set(context.getXUpdated());
			writer.write(out);
		}

		new BooleanWritable(null != context.getUInitial()).write(out);
		if (null != context.getUInitial()) {
			writer.set(context.getUInitial());
			writer.write(out);
		}

		new BooleanWritable(null != context.getZUpdated()).write(out);
		if (null != context.getZUpdated()) {
			writer.set(context.getZUpdated());
			writer.write(out);
		}

		new DoubleWritable(context.getRho()).write(out);
		new DoubleWritable(context.getLambdaValue()).write(out);
		new DoubleWritable(context.getPrimalObjectiveValue()).write(out);
		new LongWritable(context.getCount()).write(out);
	}

}
