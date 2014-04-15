package com.b5m.larser.offline.topn;

import java.util.Iterator;
import java.util.Map;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.b5m.conf.Configuration;

class ClusterInfo {
	private Integer clusterHash;
	private Vector featureVector;

	public ClusterInfo(Integer num, Map<Integer, Float> pows) {
		clusterHash = num;
		featureVector = new SequentialAccessSparseVector(Configuration
				.getInstance().getItemFeatureDimension());
		Iterator<Map.Entry<Integer, Float>> infoIterator = pows.entrySet()
				.iterator();
		while (infoIterator.hasNext()) {
			Map.Entry<Integer, Float> entry = infoIterator.next();
			featureVector.set(entry.getKey(), entry.getValue());
		}
	}

	public Integer getClusterHash() {
		return clusterHash;
	}

	public Vector getClusterFeatureVector() {
		return featureVector;
	}

}
