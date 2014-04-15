package com.b5m.msgpack;

import java.util.Iterator;
import java.util.List;

import org.msgpack.annotation.Message;

@Message
public class ClusterInfoResponse {
	private List<ClusterInfo> clusterInfo;

	public Iterator<ClusterInfo> iterator() {
		return clusterInfo.iterator();
	}

	public int size() {
		return clusterInfo.size();
	}
}
