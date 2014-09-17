package io.izenecloud.admm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdmmMapperContextWritable implements Writable {
	private AdmmMapperContext context;

	public AdmmMapperContextWritable(AdmmMapperContext context) {
		this.context = context;
	}

	public AdmmMapperContextWritable() {
	}

	public AdmmMapperContext get() {
		return context;
	}

	public void readFields(DataInput in) throws IOException {
		Text splitId = new Text();
		splitId.readFields(in);

		IntWritable len = new IntWritable();
		len.readFields(in);
		double[] u = new double[len.get()];
		for (int i = 0; i < len.get(); ++i) {
			DoubleWritable writable = new DoubleWritable();
			writable.readFields(in);
			u[i] = writable.get();
		}

		len.readFields(in);
		double[] x = new double[len.get()];
		for (int i = 0; i < len.get(); ++i) {
			DoubleWritable writable = new DoubleWritable();
			writable.readFields(in);
			x[i] = writable.get();
		}

		len.readFields(in);
		double[] z = new double[len.get()];
		for (int i = 0; i < len.get(); ++i) {
			DoubleWritable writable = new DoubleWritable();
			writable.readFields(in);
			z[i] = writable.get();
		}

		DoubleWritable rho = new DoubleWritable();
		rho.readFields(in);

		DoubleWritable lambda = new DoubleWritable();
		lambda.readFields(in);

		DoubleWritable primalObjectiveValue = new DoubleWritable();
		primalObjectiveValue.readFields(in);

		DoubleWritable rNorm = new DoubleWritable();
		rNorm.readFields(in);

		DoubleWritable sNorm = new DoubleWritable();
		sNorm.readFields(in);

		context = new AdmmMapperContext(splitId.toString(), null, null, u, x,
				z, rho.get(), lambda.get(), primalObjectiveValue.get(),
				rNorm.get(), sNorm.get());
	}

	public void write(DataOutput out) throws IOException {
		new Text(context.getSplitId()).write(out);
		double[] u = context.getUInitial();
		new IntWritable(u.length).write(out);
		for (double e : u) {
			new DoubleWritable(e).write(out);
		}
		double[] x = context.getXInitial();
		new IntWritable(x.length).write(out);
		for (double e : x) {
			new DoubleWritable(e).write(out);
		}
		double[] z = context.getZInitial();
		new IntWritable(z.length).write(out);
		for (double e : z) {
			new DoubleWritable(e).write(out);
		}

		new DoubleWritable(context.getRho()).write(out);
		new DoubleWritable(context.getLambdaValue()).write(out);
		new DoubleWritable(context.getPrimalObjectiveValue()).write(out);
		new DoubleWritable(context.getRNorm()).write(out);
		new DoubleWritable(context.getSNorm()).write(out);
	}
}