package io.izenecloud.larser.offline.topn;

import org.apache.mahout.math.Vector;

class IntVector {
	private final Integer num;
	private final Vector vec;

	public IntVector(Integer num, Vector vec) {
		this.num = num;
		this.vec = vec;
	}

	public Integer getInt() {
		return num;
	}

	public Vector getVector() {
		return vec;
	}
}
