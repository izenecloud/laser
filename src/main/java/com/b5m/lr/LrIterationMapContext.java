package com.b5m.lr;

import java.util.Iterator;
import java.util.List;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

public class LrIterationMapContext {
	private static final double LAMBDA_VALUE = 1e-6;
	private String itemId;

	private List<Vector> a;

	private double[] b;

	private double[] x;

	private double rho;

	private double lambdaValue;

	public LrIterationMapContext(String itemId, List<Vector> ab) {
		this.itemId = itemId;
		this.a = ab;
		int numCols = this.a.get(0).size() - 1;

		Iterator<Vector> iterator = this.a.iterator();
		int row = 0;
		while (iterator.hasNext()) {
			Vector v = iterator.next();
			b[row] = v.get(numCols);
			v.set(numCols, 0.0);
			row++;
		}
		x = new double[numCols];
		rho = 1.0;
		lambdaValue = LAMBDA_VALUE;
	}

	public LrIterationMapContext(String itemId, List<Vector> ab, double rho) {
		this(itemId, ab);
		this.rho = rho;
	}

	public LrIterationMapContext(String itemId, List<Vector> ab, double[] x,
			double rho, double lambdaValue) {
		this.itemId = itemId;
		this.a = ab;
		int numCols = this.a.get(0).size() - 1;
		int numRows = this.a.size();
		b = new double[numRows];
		Iterator<Vector> iterator = this.a.iterator();
		int row = 0;
		while (iterator.hasNext()) {
			Vector v = iterator.next();
			b[row] = v.get(numCols);
			v.set(numCols, 0.0);
			row++;
		}
		
		this.x = x;

		this.rho = rho;
		this.lambdaValue = lambdaValue;
	}

	public LrIterationMapContext(String itemId, List<Vector> a, double[] b,
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

	public List<Vector> getA() {
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
