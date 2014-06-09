package com.b5m.msgpack;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MsgpackRecordWriter<K, V> extends RecordWriter<K, V> {
	private MsgpackClient client = null;
	private String collection = null;
	private String method = null;

	public MsgpackRecordWriter(TaskAttemptContext context) {
		Configuration conf = context.getConfiguration();
		collection = conf.get("com.b5m.msgpack.collection");
		method = conf.get("com.b5m.msgpack.method");
		client = new MsgpackClient(conf.get("com.b5m.msgpack.ip"), conf.getInt(
				"com.b5m.msgpack.port", 0), collection);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		client.close();
	}

	@Override
	public void write(K key, V value) throws IOException {
		try {
			if (null == key || key instanceof NullWritable) {
				Object[] params = new Object[1];
				params[0] = value;

				client.writeIgnoreRetValue(params, method);

			} else {
				Object[] params = new Object[2];
				params[0] = key;
				params[1] = value;
				client.writeIgnoreRetValue(params, method);
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

}
