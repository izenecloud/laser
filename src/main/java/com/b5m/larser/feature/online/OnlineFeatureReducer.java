package com.b5m.larser.feature.online;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

import com.b5m.larser.feature.OnlineVectorWritable;
import com.b5m.lr.ListWritable;

public class OnlineFeatureReducer extends
		Reducer<Text, VectorWritable, Text, ListWritable> {

	protected void reduce(Text key, Iterable<OnlineVectorWritable> values,
			Context context) throws IOException, InterruptedException {
		List<Writable> list = new LinkedList<Writable>();
		for (OnlineVectorWritable value : values) {
			list.add(value);
		}
		context.write(key, new ListWritable(list));
	}
}
