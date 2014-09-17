package io.izenecloud.larser.feature;

import org.apache.mahout.math.Vector;

public class Request {
	private Vector userFeature;
	private Vector itemFeature;
	private Integer action;

	public Request() {

	}

	public Request(Vector userFeature, Vector itemFeature,
			Integer action) {
		this.userFeature = userFeature;
		this.itemFeature = itemFeature;
		this.action = action;
	}

	public Vector getUserFeature() {
		return userFeature;
	}

	public Vector getItemFeature() {
		return itemFeature;
	}

	public Integer getAction() {
		return action;
	}

}
