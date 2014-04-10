package com.b5m.larser.offline.topn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

class PriorityQueueWritable implements Writable {
	private PriorityQueue queue;

	public PriorityQueueWritable() {

	}

	public PriorityQueueWritable(PriorityQueue queue) {
		this.queue = queue;
	}

	public PriorityQueue get() {
		return queue;
	}

	public void write(DataOutput out) throws IOException {
		new ArrayWritable(IntDoublePairWritable.class,
				queue.toArray(new IntDoublePairWritable[0])).write(out);
	}

	public void readFields(DataInput in) throws IOException {
		ArrayWritable arr = new ArrayWritable(IntDoublePairWritable.class);
		arr.readFields(in);
		queue = new PriorityQueue(arr.get());
	}

}
