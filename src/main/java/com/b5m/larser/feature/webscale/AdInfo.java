package com.b5m.larser.feature.webscale;

import org.msgpack.annotation.Message;

import com.b5m.msgpack.SparseVector;

@Message
class AdInfo {
	public String adId;
	public String clusteringId;
	public SparseVector context;
}
