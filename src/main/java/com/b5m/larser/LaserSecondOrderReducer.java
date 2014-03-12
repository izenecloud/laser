package com.b5m.larser;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LaserSecondOrderReducer extends
		Reducer<LongWritable, Text, NullWritable, Text> {
	
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(NullWritable.get(), value);
		}
	}
}
