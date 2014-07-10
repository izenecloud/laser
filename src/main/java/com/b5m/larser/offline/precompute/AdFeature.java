package com.b5m.larser.offline.precompute;

import org.msgpack.annotation.Message;

import com.b5m.msgpack.SparseVector;

@Message
class AdFeature {
	public Long k;
	public SparseVector v;
}
