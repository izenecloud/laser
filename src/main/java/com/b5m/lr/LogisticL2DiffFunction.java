package com.b5m.lr;

import java.util.Iterator;
import java.util.List;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import edu.stanford.nlp.optimization.DiffFunction;

public class LogisticL2DiffFunction implements DiffFunction {
	private final List<Vector> a;
	private final double[] b;
	private final int m;
	private final int n;
	private final double lambda;

	public LogisticL2DiffFunction(List<Vector> a, double[] b,
			double[] xInitial, double lambda) {
		this.a = a;
		this.b = b;
		m = a.size();
		n = a.get(0).size() - 1;
		this.lambda = lambda * 2;

	}

	public double valueAt(double[] x) {
		double result = 0.0;
		// 1+ exp(-b'AX)
		Iterator<Vector> iterator = this.a.iterator();
		int row = 0;
		while (iterator.hasNext()) {
			Vector v = iterator.next();
			double ax = 0.0;
			for (Element e : v.nonZeroes()) {
				ax += e.get() * x[e.index()];
			}
			double axb = ax * b[row];
			double thisLoopResult = Math.log(1.0 + Math.exp(-axb));
			result += thisLoopResult;
			row++;
		}	
		result /= m;
		
		// l2 regularization
		double penalty = 0.0;
		for (int i = 0; i < n; i++) {
			penalty += x[i] * x[i];
		}
	
		result += penalty * this.lambda / 2;
		return result;
	}

	public int domainDimension() {
		return n;
	}

	public double[] derivativeAt(double[] x) {
		// -A'b / (1 + exp(b'Ax) + 2 lambda *x
		double[] out = new double[n];
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] = 0.0; // reset result values to 0.0
		}
		Iterator<Vector> iterator = this.a.iterator();
		int row = 0;
		while (iterator.hasNext()) {
			Vector v = iterator.next();
			double ax = 0.0;
			for (Element e : v.nonZeroes()) {
				ax += e.get() * x[e.index()];
			}
			double thisRowMultiplier = this.b[row]
					/ (1.0 + Math.exp(this.b[row] * ax));
			for (Element e : v.nonZeroes()) {
				out[e.index()] += -e.get() * thisRowMultiplier;
			}
			row++;
		}
		
		for (int i = 0; i < n; i++) {
			out[i] /= this.m;
			out[i] += this.lambda * x[i];
		}

		return out;
	}

}
