package com.b5m.msgpack;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.msgpack.annotation.Message;

import com.b5m.larser.offline.topn.IntDoublePairWritable;

@Message
public class PriorityQueue {
	private String user;
	private Map<Integer, Float> cluster;
	
	public PriorityQueue() {
		
	}

	public PriorityQueue(String user,
			com.b5m.larser.offline.topn.PriorityQueue queue) {
		this.user = user;
		this.cluster = new HashMap<Integer, Float>();
		Iterator<IntDoublePairWritable> iterator = queue.iterator();
		while (iterator.hasNext()) {
			IntDoublePairWritable v = iterator.next();
			cluster.put(v.getKey(), new Float(v.getValue()));
		}
	}
}
