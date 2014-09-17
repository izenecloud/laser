package io.izenecloud.larser.feature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class OnlineVectorWritable implements Writable {
	private Double offset;
	private Integer action;
	private Vector sample;

	public OnlineVectorWritable() {

	}

	public OnlineVectorWritable(Double offset, Integer action, Vector sample) {
		this.offset = offset;
		this.action = action;
		this.sample = sample;
	}

	public Double getOffset() {
		return offset;
	}

	public Integer getOction() {
		return action;
	}

	public Vector getSample() {
		return sample;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(offset);
		out.writeInt(action);
		VectorWritable.writeVector(out, sample);
	}

	public void readFields(DataInput in) throws IOException {
		offset = in.readDouble();
		action = in.readInt();
		sample = VectorWritable.readVector(in);
	}

}
