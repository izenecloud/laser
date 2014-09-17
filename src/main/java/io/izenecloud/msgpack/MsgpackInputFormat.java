package io.izenecloud.msgpack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MsgpackInputFormat<V> extends InputFormat<Long, V> {

	private long goalsize(Configuration conf) {
		String urlList = conf.get("com.b5m.laser.msgpack.host");
		int port = conf.getInt("com.b5m.laser.msgpack.port", 0);
		String collection = conf.get("com.b5m.laser.collection");
		String method = conf.get("com.b5m.laser.msgpack.input.method");
		MsgpackClient client = new MsgpackClient(urlList, port, collection);
		try {
			Long size = (Long) client.asyncRead(new Object[0], method + "|size",
					Long.class);
			client.close();
			return size;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		long goalSize = goalsize(conf);
		String[] urlList = conf.get("com.b5m.laser.msgpack.host").split(",");
		int numNode = urlList.length;
		List<InputSplit> splits = new ArrayList<InputSplit>();
		long splitLength = (long) Math.ceil((double) goalSize / numNode);

		long bytesRemaining = goalSize;
		for (int i = 0; i < numNode; i++) {
			if (bytesRemaining > splitLength) {
				splits.add(new FileSplit(new Path(urlList[i]), i * splitLength,
						splitLength, null));
			} else {
				splits.add(new FileSplit(new Path(urlList[i]), i * splitLength,
						bytesRemaining, null));
			}
			bytesRemaining -= splitLength;
		}
		return splits;
	}

	@Override
	public RecordReader<Long, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new MsgpackRecordReader<V>();
	}

}
