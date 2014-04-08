package com.b5m.larser.online;

import org.msgpack.annotation.Message;

import com.b5m.msgpack.MsgpackVector;

@Message
public class LaserOnlineModel {
	private Integer key;
	private Double delta;
	private MsgpackVector eta;

	public LaserOnlineModel(Integer key, Double delta, MsgpackVector eta) {
		this.key = key;
		this.delta = delta;
		this.eta = eta;
	}
}
