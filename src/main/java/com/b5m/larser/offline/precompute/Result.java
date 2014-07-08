package com.b5m.larser.offline.precompute;

import java.util.List;

import org.msgpack.annotation.Message;

@Message
class Result {
	private float betaStable;
	private List<Float> AStable;

	public Result() {

	}

	public Result(float betaStable, List<Float> AStable) {
		this.betaStable = betaStable;
		this.AStable = AStable;
	}
}
