package io.izenecloud.lr;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import edu.stanford.nlp.optimization.DiffFunction;

public class LogisticL2DiffFunction implements DiffFunction {
	private final Vector[] a;
	private final double[] b;
	private final double[] knownOffset;
	private final int m;
	private final int n;
	private final double lambda;

	public LogisticL2DiffFunction(Vector[] a, double[] b, double[] knownOffset,
			double[] xInitial, double lambda) {
		this.a = a;
		this.b = b;
		this.knownOffset = knownOffset;
		m = a.length;
		n = a[0].size() - 1;
		this.lambda = lambda * 2;
	}

	public double valueAt(double[] x) {
		double result = 0.0;
		// 1+ exp(-b'AX)
		for (int i = 0; i < this.m; i++) {
			Vector v = this.a[i];
			double ax = 0.0;
			for (Element e : v.nonZeroes()) {
				ax += e.get() * x[e.index()];
			}
			double axb = (ax + knownOffset[i]) * b[i];
			double thisLoopResult = Math.log(1.0 + Math.exp(-axb));
			result += thisLoopResult;
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

		for (int i = 0; i < this.m; i++) {
			Vector v = this.a[i];
			double ax = 0.0;
			for (Element e : v.nonZeroes()) {
				ax += e.get() * x[e.index()];
			}
			double thisRowMultiplier = this.b[i]
					/ (1.0 + Math.exp(this.b[i] * (ax + knownOffset[i])));
			for (Element e : v.nonZeroes()) {
				out[e.index()] += -e.get() * thisRowMultiplier;
			}
		}

		for (int i = 0; i < n; i++) {
			out[i] /= this.m;
			out[i] += this.lambda * x[i];
		}

		return out;
	}

}
