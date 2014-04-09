package com.b5m.larser.offline;

import org.apache.hadoop.io.Writable;

class PriorityQueue extends java.util.PriorityQueue<DoubleIntPairWritable>{

	public PriorityQueue(int initialCapacity) {
		super(initialCapacity);
	}
	
	public PriorityQueue() {
		super();
	}

	public PriorityQueue(Writable[] values) {
		for (Writable value : values) {
			super.add((DoubleIntPairWritable)value);
		}
	}

}
