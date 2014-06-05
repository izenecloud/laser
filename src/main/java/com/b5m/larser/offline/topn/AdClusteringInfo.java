package com.b5m.larser.offline.topn;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.b5m.msgpack.SparseVector;

class AdClusteringInfo {
	private Integer clusteringId;
	private Vector info;

	public AdClusteringInfo(Integer id, SparseVector sv, int dimension) {
		clusteringId = id;
		info = new SequentialAccessSparseVector(dimension);
		while (sv.hasNext()) {
			info.set(sv.getIndex(), sv.get());
		}
	}

	public Integer getClusteringId() {
		return clusteringId;
	}

	public Vector getClusteringInfo() {
		return info;
	}

}
