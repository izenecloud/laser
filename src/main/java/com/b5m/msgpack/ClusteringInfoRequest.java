package com.b5m.msgpack;

import org.msgpack.annotation.Message;

@Message
public class ClusteringInfoRequest {
	Integer clusterHash;

	public ClusteringInfoRequest() {
		clusterHash = 0;
	}

	public ClusteringInfoRequest(Integer clusterHash) {
		this.clusterHash = clusterHash;
	}
}
