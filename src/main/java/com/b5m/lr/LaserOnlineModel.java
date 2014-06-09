package com.b5m.lr;

import java.util.List;

class LaserOnlineModel {

	private float deta;
	private List<Float> eta;

	public LaserOnlineModel() {

	}

	public LaserOnlineModel(double[] x) {
		if (x.length <= 0) {
			return;
		}
		deta = (float) (x[0]);
		List<Float> args = new java.util.Vector<Float>(x.length - 1);
		for (int i = 0; i < x.length; i++) {
			args.add((float) (x[i]));
		}
	}
}
