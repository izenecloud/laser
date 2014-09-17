package io.izenecloud.larser.feature.webscale;

import io.izenecloud.msgpack.SparseVector;

import org.msgpack.annotation.Message;

@Message
class AdInfo {
	public String DOCID;
	public String clusteringId;
	public SparseVector context;
}
