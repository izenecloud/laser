package com.b5m.msgpack;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;

public class MsgpackRecordWriter<K, V> extends RecordWriter<K, V> {
	private List<Client> clients = null;
	private String method = null;

	public MsgpackRecordWriter(TaskAttemptContext context) {
		Configuration conf = context.getConfiguration();
		EventLoop loop = EventLoop.defaultEventLoop();
		clients = new LinkedList<Client>();
		try {
			String msgpacks = conf.get("com.b5m.msgpack.ip");
			for (String msgpack : msgpacks.split(",")) {
				clients.add(new Client(msgpack, conf.getInt(
						"com.b5m.msgpack.port", 0), loop));
			}

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		method = conf.get("com.b5m.msgpack.method");
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		for (Client client : clients) {
			client.close();
		}
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		if (null == key || key instanceof NullWritable) {
			Object[] params = new Object[1];
			params[0] = value;
			for (Client client : clients) {
				client.callApply(method, params);
			}

		} else {
			Object[] params = new Object[2];
			params[0] = key;
			params[1] = value;
			for (Client client : clients) {
				client.callApply(method, params);
			}
		}
	}

}
