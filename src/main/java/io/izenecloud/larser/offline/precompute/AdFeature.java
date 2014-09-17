package io.izenecloud.larser.offline.precompute;

import io.izenecloud.msgpack.SparseVector;

import org.msgpack.annotation.Message;

@Message
class AdFeature {
	public Long k;
	public SparseVector v;
}
