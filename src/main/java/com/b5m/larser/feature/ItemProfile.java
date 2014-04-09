package com.b5m.larser.feature;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import org.apache.mahout.math.Vector;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;

import com.b5m.conf.Configuration;

public class ItemProfile {
	private final RPCInterface iface;
	private final Client client;

	static interface RPCInterface {
		SplitTitleResponse spliteTitle(SplitTitleRequest request);
	}

	public ItemProfile() throws UnknownHostException {
		Configuration conf = Configuration.getInstance();
		EventLoop loop = EventLoop.defaultEventLoop();
		client = new Client(conf.getMsgpackAddress(), conf.getMsgpackPort(),
				loop);
		client.setRequestTimeout(1);

		iface = client.proxy(RPCInterface.class);
	}

	public void close() {
		client.close();
	}

	public void setItemFeature(String title, Vector item) {
		Map<Integer, Float> res = iface.spliteTitle(new SplitTitleRequest(title)).getResponse();
		Iterator<Map.Entry<Integer, Float>> iterator = res.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, Float> entry = iterator.next();
			item.set(entry.getKey(), entry.getValue());
		}
	}
}
