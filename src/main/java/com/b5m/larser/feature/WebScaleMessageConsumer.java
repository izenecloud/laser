package com.b5m.larser.feature;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.msgpack.MessagePack;
import org.msgpack.type.Value;

import com.b5m.flume.B5MEvent;
import com.b5m.msgpack.MsgpackClient;
import com.b5m.msgpack.SparseVector;

public class WebScaleMessageConsumer extends LaserMessageConsumer {
	private final MsgpackClient client;

	public WebScaleMessageConsumer(String collection, Path output,
			FileSystem fs, Configuration conf) throws IOException {
		super(collection, output, fs, conf);
		client = new MsgpackClient(com.b5m.conf.Configuration.getInstance()
				.getMsgpackAddress(collection), com.b5m.conf.Configuration
				.getInstance().getMsgpackPort(collection), collection);
	}

	@Override
	public boolean write(B5MEvent b5mEvent) throws IOException {
		// TODO
		// user feature
		// item feature
		// item id
		// action
		Vector ad = new SequentialAccessSparseVector();
		Object[] req = new Object[1];
		req[0] = new String("item");
		Value res = client.asyncRead(req, "itemFeature");
		SparseVector vec = new org.msgpack.unpacker.Converter(
				new MessagePack(), res).read(SparseVector.class);

		Iterator<Entry<Integer, Float>> iterator = vec.vec.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Entry<Integer, Float> entry = iterator.next();
			ad.set(entry.getKey(), entry.getValue());
		}
		append(new Text(""), new RequestWritable(
				new SequentialAccessSparseVector(), ad, -1));
		if (-1 != vec.clustering) {
			append(new Text(Integer.toString(vec.clustering) + "_per_clustering"),
					new RequestWritable(new SequentialAccessSparseVector(), ad,
							-1));
		}
		return false;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
	}
}
