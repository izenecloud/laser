package com.b5m.larser.offline;

import org.msgpack.annotation.Message;

import com.b5m.msgpack.MsgpackMatrix;
import com.b5m.msgpack.MsgpackVector;

@Message
public class LaserOfflineModel {
	private MsgpackVector alpha;
	private MsgpackVector beta;
	private MsgpackMatrix A;

	public LaserOfflineModel(MsgpackVector alpha, MsgpackVector beta,
			MsgpackMatrix A) {
		this.alpha = alpha;
		this.beta = beta;
		this.A = A;
	}
}
