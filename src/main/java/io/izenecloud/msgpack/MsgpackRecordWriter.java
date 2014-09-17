package io.izenecloud.msgpack;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MsgpackRecordWriter<K, V> extends RecordWriter<K, V> {
	private MsgpackClient client = null;
	private String collection = null;
	private String method = null;
	final Object[] kvReq = new Object[2];
	final Object[] vReq = new Object[1];

	public MsgpackRecordWriter(TaskAttemptContext context) {
		Configuration conf = context.getConfiguration();
		collection = conf.get("com.b5m.laser.collection");
		method = conf.get("com.b5m.laser.msgpack.output.method");
		client = new MsgpackClient(conf.get("com.b5m.laser.msgpack.host"), conf.getInt(
				"com.b5m.laser.msgpack.port", 0), collection);
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
				vReq[0] = value;
				client.writeIgnoreRetValue(vReq, method);
				value = null;

			} else {
				kvReq[0] = key;
				kvReq[1] = value;
				client.writeIgnoreRetValue(kvReq, method);
				key = null;
				value = null;
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

}
