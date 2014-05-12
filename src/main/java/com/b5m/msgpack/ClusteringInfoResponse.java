package com.b5m.msgpack;

import java.util.Iterator;
import java.util.List;

import org.msgpack.annotation.Message;

@Message
public class ClusteringInfoResponse {
	private List<ClusteringInfo> clusteringInfo;

	public Iterator<ClusteringInfo> iterator() {
		return clusteringInfo.iterator();
	}

	public int size() {
		return clusteringInfo.size();
	}
}
