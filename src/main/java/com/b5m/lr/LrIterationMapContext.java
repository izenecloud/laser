package com.b5m.lr;

import org.apache.mahout.math.Vector;

public class LrIterationMapContext {
	private static final double LAMBDA_VALUE = 1e-6;
	private int itemId;

	private Vector[] a;

	private double[] b;

	private double[] x;

	private double rho;

	private double lambdaValue;

	public LrIterationMapContext(int itemId, Vector[] ab) {
		this.itemId = itemId;
		this.a = ab;
		int numCols = this.a[0].size() - 1;
		int numRows = this.a.length;
		this.b = new double[numRows];
		
		for (int row = 0; row < numRows; row++) {
			this.b[row] = this.a[row].get(numCols);
			this.a[row].set(numCols, 0.0);
		}
		x = new double[numCols];
		rho = 1.0;
		lambdaValue = LAMBDA_VALUE;
	}

	public LrIterationMapContext(int itemId, Vector[] ab, double rho) {
		this(itemId, ab);
		this.rho = rho;
	}

	public LrIterationMapContext(int itemId, Vector[] ab, double[] x,
			double rho, double lambdaValue) {
		this.itemId = itemId;
		this.a = ab;
		int numCols = this.a[0].size() - 1;
		int numRows = this.a.length;
		this.b = new double[numRows];
		
		for (int row = 0; row < numRows; row++) {
			this.b[row] = this.a[row].get(numCols);
			this.a[row].set(numCols, 0.0);
		}

		this.x = x;

		this.rho = rho;
		this.lambdaValue = lambdaValue;
	}

	public LrIterationMapContext(int itemId, Vector[] a, double[] b,
			double[] x, double rho, double lambdaValue) {
		this.itemId = itemId;
		this.a = a;
		this.b = b;
		this.x = x;
		this.rho = rho;
		this.lambdaValue = lambdaValue;
	}

	public LrIterationMapContext() {
	}

	public void setLrIterationMapContext(LrIterationMapContext context) {
		this.itemId = context.itemId;
		this.a = context.a;
		this.b = context.b;
		this.x = context.x;
		this.rho = context.rho;
		this.lambdaValue = context.lambdaValue;
	}

	public Vector[] getA() {
		return a;
	}

	public double[] getB() {
		return b;
	}

	public double[] getX() {
		return x;
	}

	public void setX(double[] x) {
		this.x = x;
	}

	public double getRho() {
		return rho;
	}

	public void setRho(double rho) {
		this.rho = rho;
	}

	public double getLambdaValue() {
		return lambdaValue;
	}
}
