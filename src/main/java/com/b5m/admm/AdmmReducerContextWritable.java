package com.b5m.admm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdmmReducerContextWritable implements Writable {
	private AdmmReducerContext context;

	public AdmmReducerContextWritable(AdmmReducerContext context) {
		this.context = context;
	}

	public AdmmReducerContextWritable() {
	}
	
	public AdmmReducerContext get() {
		return context;
	}

	public void readFields(DataInput in) throws IOException {
		Text splitId = new Text();
		splitId.readFields(in);

		IntWritable len = new IntWritable();
		len.readFields(in);
		double[] uInitial = new double[len.get()];
		for (int i = 0; i < len.get(); ++i) {
			DoubleWritable writable = new DoubleWritable();
			writable.readFields(in);
			uInitial[i] = writable.get();
		}

		len.readFields(in);
		double[] xInitial = new double[len.get()];
		for (int i = 0; i < len.get(); ++i) {
			DoubleWritable writable = new DoubleWritable();
			writable.readFields(in);
			xInitial[i] = writable.get();
		}

		len.readFields(in);
		double[] xUpdated = new double[len.get()];
		for (int i = 0; i < len.get(); ++i) {
			DoubleWritable writable = new DoubleWritable();
			writable.readFields(in);
			xUpdated[i] = writable.get();
		}

		len.readFields(in);
		double[] zInitial = new double[len.get()];
		for (int i = 0; i < len.get(); ++i) {
			DoubleWritable writable = new DoubleWritable();
			writable.readFields(in);
			zInitial[i] = writable.get();
		}

		DoubleWritable rho = new DoubleWritable();
		rho.readFields(in);

		DoubleWritable lambda = new DoubleWritable();
		lambda.readFields(in);

		DoubleWritable primalObjectiveValue = new DoubleWritable();
		primalObjectiveValue.readFields(in);

		context = new AdmmReducerContext(splitId.toString(), uInitial, xInitial, xUpdated,
				zInitial, primalObjectiveValue.get(), rho.get(), lambda.get());
	}

	public void write(DataOutput out) throws IOException {
		new Text(context.getSplitId()).write(out);
		
		double[] uInitial = context.getUInitial();
		new IntWritable(uInitial.length).write(out);
		for (double e : uInitial) {
			new DoubleWritable(e).write(out);
		}
		double[] xInitial = context.getXInitial();
		new IntWritable(xInitial.length).write(out);
		for (double e : xInitial) {
			new DoubleWritable(e).write(out);
		}
		double[] xUpdated = context.getXUpdated();
		new IntWritable(xUpdated.length).write(out);
		for (double e : xUpdated) {
			new DoubleWritable(e).write(out);
		}
		double[] zInitial = context.getZInitial();
		new IntWritable(zInitial.length).write(out);
		for (double e : zInitial) {
			new DoubleWritable(e).write(out);
		}

		new DoubleWritable(context.getRho()).write(out);
		new DoubleWritable(context.getLambdaValue()).write(out);
		new DoubleWritable(context.getPrimalObjectiveValue()).write(out);
	}

}
