package io.izenecloud.msgpack;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MsgpackRecordReader<V> extends RecordReader<Long, V> {
	private MsgpackClient client = null;
	private String method = null;
	private long start = 0;
	private long splitLenth = 0;
	private long readLength = 0;
	private V  v = null;
	private Class<?> vClass = null;
	private final Object[] req = new Object[0];
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		String host = fileSplit.getPath().toString();
		Configuration conf = context.getConfiguration();
		String collection = conf.get("com.b5m.laser.collection");
		int port = conf.getInt("com.b5m.laser.msgpack.port", 0);
		method = conf.get("com.b5m.laser.msgpack.input.method");
		client = new MsgpackClient(host, port, collection);
		start = fileSplit.getStart();
		splitLenth = fileSplit.getLength();
		readLength = 0;
		vClass = conf.getClass("com.b5m.laser.msgpack.input.value.class", null);
		try {
			Object[] req = new Object[1];
			req[0] = start;
			client.writeIgnoreRetValue(req, method + "|start");
		} catch (Exception e) {
			throw new IOException(e.getLocalizedMessage());
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {		
		v = (V)client.asyncRead(req, method + "|next", vClass);
		readLength++;
		return true;
	}

	@Override
	public Long getCurrentKey() throws IOException, InterruptedException {
		return readLength;
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
		return v;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return readLength / (float) splitLenth;
	}

	@Override
	public void close() throws IOException {
		client.close();
	}

}
