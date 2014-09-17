package io.izenecloud.lr;

import java.util.List;

import org.msgpack.annotation.Message;

@Message
public class LaserOnlineModel {

	private float deta;
	private List<Float> eta;

	public LaserOnlineModel() {

	}

	public LaserOnlineModel(double[] x) {
		if (x.length <= 0) {
			return;
		}
		deta = (float) (x[0]);
		eta = new java.util.Vector<Float>(x.length - 1);
		for (int i = 0; i < x.length; i++) {
			eta.add((float) (x[i]));
		}
	}
}
