package com.b5m.msgpack;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;

public class MsgpackRecordWriter<K, V> extends RecordWriter<K, V> {
	private Client client = null;
	private String method = null;

	public MsgpackRecordWriter(TaskAttemptContext context) {
		Configuration conf = context.getConfiguration();
		EventLoop loop = EventLoop.defaultEventLoop();
		try {
			client = new Client(conf.get("com.b5m.msgpack.ip"), conf.getInt(
					"com.b5m.msgpack.port", 0), loop);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		method = conf.get("com.b5m.msgpack.method");
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		client.close();
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// System.out.println(value.getClass());
		if (null == key) {
			Object[] params = new Object[1];
			params[0] = value;
			client.callApply(method, params);
		} else {
			Object[] params = new Object[2];
			params[0] = key;
			params[1] = value;
			client.callApply(method, params);
		}
	}

}
