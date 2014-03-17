package com.b5m.lr;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

public class LrIterationMapContext {
	private static final double LAMBDA_VALUE = 1e-6;
	private String itemId;

	private Matrix a;

	private double[] b;

	private double[] x;

	private double rho;

	private double lambdaValue;

	public LrIterationMapContext(String itemId, Matrix ab) {
		this.itemId = itemId;

		int numCols = ab.numCols() - 1;
		a = ab.like(ab.numRows(), ab.numCols() - 1);
		for (int row = 0; row < a.numRows(); row++) {
			Vector v = ab.viewRow(row);
			Vector av = a.viewRow(row);
			for (Element e : v.nonZeroes()) {
				if (numCols > e.index()) {
					av.setQuick(e.index(), e.get());
				}
			}
		}
		b = new double[ab.numRows()];
		for (int row = 0; row < ab.numRows(); row++) {
			b[row] = ab.get(row, numCols);
		}
		x = new double[numCols];
		rho = 1.0;
		lambdaValue = LAMBDA_VALUE;
	}

	public LrIterationMapContext(String itemId, Matrix ab, double rho) {
		this(itemId, ab);
		this.rho = rho;
	}

	public LrIterationMapContext(String itemId, Matrix ab, double[] x,
			double rho, double lambdaValue) {
		this.itemId = itemId;
		int numCols = ab.numCols() - 1;
		a = ab.like(ab.numRows(), numCols);
		b = new double[ab.numRows()];
		for (int row = 0; row < a.numRows(); row++) {
			Vector v = ab.viewRow(row);
			b[row] = v.get(numCols);
			Vector av = a.viewRow(row);
			for (Element e : v.nonZeroes()) {
				if (numCols > e.index()) {
					av.setQuick(e.index(), e.get());
				}
			}
		}

		this.x = x;

		this.rho = rho;
		this.lambdaValue = lambdaValue;
	}

	public LrIterationMapContext(String itemId, Matrix a, double[] b,
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

	public Matrix getA() {
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
