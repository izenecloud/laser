package com.b5m.larser;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

public class LaserFirstOrderReducer extends
		Reducer<NullWritable, VectorWritable, NullWritable, VectorWritable> {
	protected void reduce(NullWritable key, Iterable<VectorWritable> values, Context context)
			throws IOException, InterruptedException {
		Vector res = null;
		for (VectorWritable value : values) {
			if (null == res) {
				res = value.get().like();
			}
			res.assign(value.get(), Functions.PLUS);
		}
		context.write(NullWritable.get(), new VectorWritable(res));
	}
}
