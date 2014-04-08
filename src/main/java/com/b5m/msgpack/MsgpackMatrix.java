package com.b5m.msgpack;

import java.util.Vector;

import org.msgpack.annotation.Message;

@Message
public class MsgpackMatrix {
	private Vector<Double>[] matrix;

	public MsgpackMatrix(Integer numRows, Integer numCols) {
		matrix = new Vector[numRows];
		for (int row = 0; row < numRows; row++) {
			matrix[row] = new Vector<Double>(numCols);
		}
	}

	public void set(Integer row, Integer col, Double v) {
		matrix[row].set(col, v);
	}
}
