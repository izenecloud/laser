package com.b5m.larser.feature.webscale;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;

import com.b5m.flume.B5MEvent;
import com.b5m.larser.feature.LaserMessageConsumer;
import com.b5m.larser.feature.OnlineVectorWritable;
import com.b5m.larser.feature.Request;
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
		// ad feature
		// ad id
		// action
		Vector ad = new SequentialAccessSparseVector();
		Object[] req = new Object[1];
		req[0] = new String("DOCID");
		AdInfo adInfo = (AdInfo) client.asyncRead(req, "getAdInfoById", AdInfo.class);

		while (adInfo.context.hasNext()) {
			ad.set(adInfo.context.getIndex(), adInfo.context.get());
		}
		Vector user = new SequentialAccessSparseVector();
		Integer action = 0;
		
		Request request = new Request(user, ad, action);
		Text key = new Text("");
		appendOffline(key, request);

		Double offset = knownOffset(request);
		OnlineVectorWritable online = new OnlineVectorWritable(offset, action, user);
		appendOnline(key, online);
		
		// clustering only for per-clustering model
		// does not need item feature.
		if (!adInfo.clusteringId.isEmpty()) {
			appendOnline(new Text(adInfo.clusteringId), online);
		}
		return false;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public String modelType() {
		return "per-ad";
	}
}
