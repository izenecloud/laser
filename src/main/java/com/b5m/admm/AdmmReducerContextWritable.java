package com.b5m.admm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
		writer.readFields(in);

		double[] zUpdated = writer.get();

		DoubleWritable rho = new DoubleWritable();
		rho.readFields(in);

		DoubleWritable lambda = new DoubleWritable();
		lambda.readFields(in);

		DoubleWritable primalObjectiveValue = new DoubleWritable();
		primalObjectiveValue.readFields(in);

		LongWritable count = new LongWritable();
		count.readFields(in);

		context = new AdmmReducerContext(null, null, null, zUpdated,
				primalObjectiveValue.get(), rho.get(), lambda.get(),
				count.get());
	}

	public void write(DataOutput out) throws IOException {
		writer.set(context.getZUpdated());
		writer.write(out);

		new DoubleWritable(context.getRho()).write(out);
		new DoubleWritable(context.getLambdaValue()).write(out);
		new DoubleWritable(context.getPrimalObjectiveValue()).write(out);
		new LongWritable(context.getCount()).write(out);
	}

}
