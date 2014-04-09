package com.b5m.larser.feature;

public class IntDoublePair {
	private int key;
	private double value;

	public IntDoublePair() {
	}

	public IntDoublePair(int k, double v) {
		this.key = k;
		this.value = v;
	}

	public void setKey(int k) {
		this.key = k;
	}

	public void setValue(double v) {
		this.value = v;
	}

	public int getKey() {
		return key;
	}

	public double getValue() {
		return value;
	}

	public int compareTo(IntDoublePair o) {
		if (key > o.key) {
			return 1;
		} else if (key < o.key) {
			return -1;
		}
		if (this.value > o.value) {
			return 1;
		} else if (this.value < o.value) {
			return -1;
		}
		return 0;
	}
}
