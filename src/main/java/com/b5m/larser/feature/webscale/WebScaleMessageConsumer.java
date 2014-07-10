package com.b5m.larser.feature.webscale;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
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
	private static final Utf8 DOCID_TAG = new Utf8("dd");
	private static final Random RANDOM = new Random();
	private final Integer adFeatureDimension;
	private final Integer userFeatureDimension;

	public WebScaleMessageConsumer(String collection, Path output,
			FileSystem fs, Configuration conf) throws IOException {
		super(collection, output, fs, conf);
		client = new MsgpackClient(com.b5m.conf.Configuration.getInstance()
				.getMsgpackAddress(collection), com.b5m.conf.Configuration
				.getInstance().getMsgpackPort(collection), collection);
		adFeatureDimension = com.b5m.conf.Configuration.getInstance()
				.getItemFeatureDimension(collection);
		userFeatureDimension = com.b5m.conf.Configuration.getInstance()
				.getUserFeatureDimension(collection);
	}

	@Override
	public boolean write(B5MEvent b5mEvent) throws IOException {
		CharSequence str = b5mEvent.getArgs().get(DOCID_TAG);
		if (null == str) {
			return true;
		}
		String DOCID = str.toString();

		Vector ad = new SequentialAccessSparseVector(adFeatureDimension);
		Object[] req = new Object[1];
		req[0] = DOCID;
		AdInfo adInfo = (AdInfo) client.asyncRead(req, "getAdInfoByDOCID",
				AdInfo.class);
		if (adInfo.DOCID.isEmpty()) {
			return true;
		}

		while (adInfo.context.hasNext()) {
			ad.set(adInfo.context.getIndex(), adInfo.context.get());
		}
		//TODO
		Vector user = new DenseVector(userFeatureDimension);
		for (int i = 0; i < userFeatureDimension; i++) {
			user.set(i, RANDOM.nextDouble());
		}
		//TODO
		Integer action = 0;

		Request request = new Request(user, ad, action);
		Text key = new Text(DOCID);
		appendOffline(key, request);

		Double offset = knownOffset(request);
		OnlineVectorWritable online = new OnlineVectorWritable(offset, action,
				user);
		appendOnline(key, online);

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
