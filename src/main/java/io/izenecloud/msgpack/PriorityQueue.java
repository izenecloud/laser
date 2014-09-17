package io.izenecloud.msgpack;

import io.izenecloud.larser.offline.topn.IntDoublePairWritable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.msgpack.annotation.Message;

@Message
public class PriorityQueue {
	private String user;
	private Map<Integer, Float> cluster;
	
	public PriorityQueue() {
		
	}

	public PriorityQueue(String user,
			io.izenecloud.larser.offline.topn.PriorityQueue queue) {
		this.user = user;
		this.cluster = new HashMap<Integer, Float>();
		Iterator<IntDoublePairWritable> iterator = queue.iterator();
		while (iterator.hasNext()) {
			IntDoublePairWritable v = iterator.next();
			cluster.put(v.getKey(), new Float(v.getValue()));
		}
	}
}
