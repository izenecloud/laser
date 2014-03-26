package com.b5m.admm;

//import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.nlp.optimization.DiffFunction;

/*
 * f(x) = (1 / m) * log[1 + exp(-b'A'x)] + rho / 2 * (x - z + u)^2
 * df/dx = (1 / m) * (-Ab) / [1 + exp(b'A'x)] + rho * (x -z + u)
 */

public class LogisticL2DiffFunction implements DiffFunction {
	private static final Logger LOG = LoggerFactory
			.getLogger(LogisticL2DiffFunction.class.getName());
	private Vector[] a; // m by n matrix of features
	private double[] b; // m by 1 vector of labels
	private double[] u;
	private double[] z;
	// private Vector nonZeros;
	private double rho;
	private int m; // number of samples
	private int n; // number of features (assumed that feature 0 is the
					// intercept)

	/*
	 * nonZeros more than 10 minutes, less than 1 second after remove
	 */

	public LogisticL2DiffFunction(Vector[] a, double[] b, double rho,
			double[] u, double[] z) {
		LOG.info("Initialize LogisticL2DiffFunction");
		this.a = a;
		this.b = b;
		this.rho = rho;
		this.m = a.length;
		if (this.m > 0) {
			this.n = this.a[0].size() - 1;
		} else {
			this.n = 0;
		}
		this.u = u;
		this.z = z;
		LOG.info("Initialize LogisticL2DiffFunction Finish");

	}

	public double[] derivativeAt(double[] x) {
		long sTime = System.nanoTime();
		double[] out = new double[x.length];
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] = 0.0; // reset result values to 0.0
		}

		for (int row = 0; row < this.m; row++) {
			Vector v = this.a[row];
			double ax = 0.0;
			for (Element e : v.nonZeroes()) {
				ax += e.get() * x[e.index()];
			}
			double thisRowMultiplier = this.b[row]
					/ (1.0 + Math.exp(this.b[row] * ax));
			for (Element e : v.nonZeroes()) {
				out[e.index()] += -e.get() * thisRowMultiplier;
			}
		}
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] /= this.m;
		}
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] += this.rho
					* (x[vectorIndex] - this.z[vectorIndex] + this.u[vectorIndex]);
		}
		LOG.info("Time for Evalute Gradient: = {}", System.nanoTime() - sTime);
		return out;
	}

	public double valueAt(double[] x) {
		return evaluate(x);
	}

	public double evaluate(double[] x) {
		double result = evaluatePrimalObjective(x);
		result += evaluateObjectiveDualPenalty(x);
		return result;
	}

	public double evaluatePrimalObjective(double[] x) {
		double result = 0.0;
		for (int row = 0; row < this.m; row++) {
			Vector v = this.a[row];
			double ax = 0;
			for (Element e : v.nonZeroes()) {
				// Calculate dot product: ai'*x, where i ai denotes the ith row
				// of a
				ax += e.get() * x[e.index()];
			}
			double axb = ax * b[row];
			double thisLoopResult = Math.log(1.0 + Math.exp(-axb));
			result += thisLoopResult;
		}
		result /= m;
		return result;
	}

	public double evaluateObjectiveDualPenalty(double[] x) {
		double xzuNorm = 0.0;
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			xzuNorm += Math.pow(x[vectorIndex] - z[vectorIndex]
					+ u[vectorIndex], 2.0);
		}
		double xzuNormScaled = xzuNorm * this.rho / 2.0;
		return xzuNormScaled;
	}

	public int domainDimension() {
		return n;
	}
}