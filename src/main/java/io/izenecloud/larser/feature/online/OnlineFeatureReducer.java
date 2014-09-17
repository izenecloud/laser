package io.izenecloud.larser.feature.online;

import io.izenecloud.larser.feature.OnlineVectorWritable;
import io.izenecloud.lr.ListWritable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class OnlineFeatureReducer extends
		Reducer<Text, OnlineVectorWritable, Text, ListWritable> {

	protected void reduce(Text key, Iterable<OnlineVectorWritable> values,
			Context context) throws IOException, InterruptedException {
		List<Writable> list = new LinkedList<Writable>();
		for (OnlineVectorWritable value : values) {
			list.add(value);
		}
		context.write(key, new ListWritable(list));
	}
}
