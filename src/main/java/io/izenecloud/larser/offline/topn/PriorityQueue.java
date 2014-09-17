package io.izenecloud.larser.offline.topn;

import org.apache.hadoop.io.Writable;

public class PriorityQueue extends java.util.PriorityQueue<IntDoublePairWritable> {

	public PriorityQueue(int initialCapacity) {
		super(initialCapacity);
	}

	public PriorityQueue() {
		super();
	}

	public PriorityQueue(Writable[] values) {
		for (Writable value : values) {
			super.add((IntDoublePairWritable) value);
		}
	}

}
