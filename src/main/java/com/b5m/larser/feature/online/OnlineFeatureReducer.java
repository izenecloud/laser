package com.b5m.larser.feature.online;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

import com.b5m.lr.ListWritable;

public class OnlineFeatureReducer extends
		Reducer<IntWritable, VectorWritable, IntWritable, ListWritable> {

	protected void reduce(IntWritable key, Iterable<VectorWritable> values,
			Context context) throws IOException, InterruptedException {
		List<Writable> list = new LinkedList<Writable>();
		for (VectorWritable value : values) {
			list.add(value);
		}
		context.write(key, new ListWritable(list));
	}
}
