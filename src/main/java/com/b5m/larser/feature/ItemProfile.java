package com.b5m.larser.feature;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import org.apache.mahout.math.Vector;

import com.b5m.msgpack.RpcClient;
import com.b5m.msgpack.SplitTitleRequest;

public class ItemProfile {

	public static void setItemFeature(String title, Vector item) {
		Map<Integer, Float> res = null;
		try {
			res = RpcClient.getInstance()
					.spliteTitle(new SplitTitleRequest(title)).getResponse();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Iterator<Map.Entry<Integer, Float>> iterator = res.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, Float> entry = iterator.next();
			item.set(entry.getKey(), entry.getValue());
		}
	}
}
