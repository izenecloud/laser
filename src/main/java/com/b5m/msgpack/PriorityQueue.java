package com.b5m.msgpack;

import java.util.Map;

import org.msgpack.annotation.Message;

@Message
public class PriorityQueue {
	private String user;
	private Map<Integer, Double> cluster;

	public PriorityQueue(String user, Map<Integer, Double> cluster) {
		this.user = user;
		this.cluster = cluster;
	}
}
