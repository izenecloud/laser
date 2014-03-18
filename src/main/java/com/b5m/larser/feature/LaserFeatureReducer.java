package com.b5m.larser.feature;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.mahout.math.VectorWritable;

public class LaserFeatureReducer
		extends
		Reducer<IntLongPairWritable, VectorWritable, LongWritable, VectorWritable> {

	private RecordWriter<LongWritable, VectorWritable> writer = null;
	private int aid = -1;

	@SuppressWarnings("unchecked")
	protected void reduce(IntLongPairWritable key,
			Iterable<VectorWritable> values, Context context)
			throws IOException, InterruptedException {
		context.getConfiguration().set("laser.feature.output.name",
				Integer.toString(key.get().getInt()));
		if (null == writer) {
			try {
				writer = (RecordWriter<LongWritable, VectorWritable>) context
						.getOutputFormatClass().newInstance()
						.getRecordWriter(context);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			aid = key.get().getInt();
		}
		if (key.get().getInt() != aid) {
			writer.close(context);
			try {
				writer = (RecordWriter<LongWritable, VectorWritable>) context
						.getOutputFormatClass().newInstance()
						.getRecordWriter(context);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			aid = key.get().getInt();
		}

		for (VectorWritable value : values) {
			writer.write(new LongWritable(key.get().getLong()), value);
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		writer.close(context);
	}
}
