package com.b5m.msgpack;

import java.util.Vector;

import org.msgpack.annotation.Message;

@Message
public class MsgpackVector {
	private Vector<Double> vec;

	public MsgpackVector(int capacity) {
		vec = new Vector<Double>(capacity);
	}

	public void set(Integer i, Double v) {
		vec.set(i, v);
	}
}
