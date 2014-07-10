package com.b5m.couchbase;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CouchbaseIterationMapper extends
		Mapper<BytesWritable, BytesWritable, Text, Text> {

	protected void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.get()), new Text(value.get()));
	}

}
