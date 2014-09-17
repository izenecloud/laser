package io.izenecloud.larser.feature.online;

import io.izenecloud.larser.feature.OnlineVectorWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OnlineFeatureMapper extends
		Mapper<Text, OnlineVectorWritable, Text, OnlineVectorWritable> {

	protected void map(Text key, OnlineVectorWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
