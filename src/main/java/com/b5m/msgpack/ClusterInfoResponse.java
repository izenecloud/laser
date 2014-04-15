package com.b5m.msgpack;

import java.util.Iterator;
import java.util.Vector;

import org.msgpack.annotation.Message;

@Message
public class ClusterInfoResponse {
	private Vector<ClusterInfo> clusterInfo;

	public Iterator<ClusterInfo> iterator() {
		return clusterInfo.iterator();
	}

	public int size() {
		return clusterInfo.size();
	}
}
