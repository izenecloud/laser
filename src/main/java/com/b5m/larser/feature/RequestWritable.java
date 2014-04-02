package com.b5m.larser.feature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class RequestWritable implements Writable {
	private Vector userFeature;
	private Vector itemFeature;
	private Integer action;

	public RequestWritable() {

	}

	public RequestWritable(Vector userFeature, Vector itemFeature,
			Integer action) {
		this.userFeature = userFeature;
		this.itemFeature = itemFeature;
		this.action = action;
	}

	public void write(DataOutput out) throws IOException {
		VectorWritable.writeVector(out, userFeature);
		VectorWritable.writeVector(out, itemFeature);
		out.writeInt(action);
	}

	public void readFields(DataInput in) throws IOException {
		userFeature = VectorWritable.readVector(in);
		itemFeature = VectorWritable.readVector(in);
		action = in.readInt();
	}

	public Vector getUserFeature() {
		return userFeature;
	}

	public Vector getItemFeature() {
		return itemFeature;
	}

	public Integer getAction() {
		return action;
	}

}
