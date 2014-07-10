package com.b5m.larser.feature.online;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.b5m.larser.feature.OnlineVectorWritable;

public class OnlineFeatureMapper extends
		Mapper<Text, OnlineVectorWritable, Text, OnlineVectorWritable> {

	protected void map(Text key, OnlineVectorWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
