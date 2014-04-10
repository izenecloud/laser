package com.b5m.msgpack;

import org.msgpack.annotation.Message;

@Message
public class ClusterInfoRequest {
	Integer clusterHash;

	public ClusterInfoRequest() {
		clusterHash = 0;
	}

	public ClusterInfoRequest(Integer clusterHash) {
		this.clusterHash = clusterHash;
	}
}
