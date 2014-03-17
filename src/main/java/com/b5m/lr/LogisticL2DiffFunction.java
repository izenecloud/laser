package com.b5m.lr;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import edu.stanford.nlp.optimization.DiffFunction;

public class LogisticL2DiffFunction implements DiffFunction {
	private final Matrix a;
	private final double[] b;
	private final double[] x;
	private final int m;
	private final int n;
	private final double lambda;
	private final Vector nonZeros;
	public LogisticL2DiffFunction(Matrix a, double[] b, double[] xInitial, double lambda) {
		this.a = a;
		this.b = b;
		this.x = xInitial;
		m = a.numRows();
		n = a.numCols();
		this.lambda = lambda;
		nonZeros = new SequentialAccessSparseVector(n);
		for (int row = 0; row < m; row++) {
			for (Element e : this.a.viewRow(row).nonZeroes()) {
				nonZeros.set(e.index(), 1.0);
			}
		}
	}

	public double valueAt(double[] x) {		
		double result = 0.0;
		// 1+ exp(-bAX)
		for (int row = 0; row < this.m; row++) {
			Vector v = a.viewRow(row);
			double ax = 0.0;
			for (Element e : v.nonZeroes()) {
				ax += e.get() * x[e.index()];
			}
			double axb = ax * b[row];
			double thisLoopResult = Math.log(1.0 + Math.exp(-axb));
			result += thisLoopResult; 
		}
		result /= m;
		// l2 regularization
		// TODO
		double penalty = 0.0;
		for (int i = 0; i < this.x.length; i++) {
			penalty += this.x[i] * this.x[i];
		}
		result += penalty * this.lambda;
		return result;
	}

	public int domainDimension() {
		return n;
	}

	public double[] derivativeAt(double[] x) {
		//-A'b / (1 + exp(b'Ax) + 2 lambda *x
		
		double[] out = new double[x.length];
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] = 0.0; // reset result values to 0.0
		}
		for (int row = 0; row < this.m; row++) {
			double ax = 0.0;
			for (Element e : this.a.viewRow(row).nonZeroes()) {
				ax += e.get() * x[e.index()];
			}
			double thisRowMultiplier = this.b[row]
					/ (1.0 + Math.exp(this.b[row] * ax));
			for (Element e : this.a.viewRow(row).nonZeroes()) {
				out[e.index()] += -e.get() * thisRowMultiplier;
			}
		}
		for (Element e : this.nonZeros.nonZeroes()) {
			out[e.index()] /= this.m;
		}
		//l2 regularization
		for (Element e : this.nonZeros.nonZeroes()) {
			out[e.index()] += this.lambda * 2
					* x[e.index()];
		}
		return out;
	}

}
