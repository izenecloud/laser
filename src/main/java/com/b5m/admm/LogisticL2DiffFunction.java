package com.b5m.admm;

import java.util.Iterator;
import java.util.List;

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
	private List<Vector> a; // m by n matrix of features
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

	public LogisticL2DiffFunction(List<Vector> a, double[] b, double rho,
			double[] u, double[] z) {
		LOG.info("Initialize LogisticL2DiffFunction");
		this.a = a;
		this.b = b;
		this.rho = rho;
		this.m = a.size();
		if (this.m > 0) {
			this.n = this.a.get(0).size();
		} else {
			this.n = 0;
		}
		this.u = u;
		this.z = z;
		/*
		 * this.nonZeros = new SequentialAccessSparseVector(this.n);
		 * Iterator<Vector> iterator = this.a.iterator(); while
		 * (iterator.hasNext()) { for (Element e : iterator.next().nonZeroes())
		 * { this.nonZeros.set(e.index(), e.get()); } }
		 */
		LOG.info("Initialize LogisticL2DiffFunction Finish");

	}

	/*
	 * 28s for 1G
	 */
	public double[] derivativeAt(double[] x) {
		long sTime = System.nanoTime();
		double[] out = new double[x.length];
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] = 0.0; // reset result values to 0.0
		}

		int row = 0;
		Iterator<Vector> iterator = this.a.iterator();
		while (iterator.hasNext()) {
			double ax = 0.0;
			Vector v = iterator.next();
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
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] /= this.m;
		}
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] += this.rho
					* (x[vectorIndex] - this.z[vectorIndex] + this.u[vectorIndex]);
		}
		//
		// for (Element e : this.nonZeros.nonZeroes()) {
		// out[e.index()] /= this.m;
		// }
		// for (Element e : this.nonZeros.nonZeroes()) {
		// out[e.index()] += this.rho
		// * (x[e.index()] - this.z[e.index()] + this.u[e.index()]);
		// }
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
		int row = 0;
		Iterator<Vector> iterator = this.a.iterator();
		while (iterator.hasNext()) {
			double ax = 0;
			for (Element e : iterator.next().nonZeroes()) {
				// Calculate dot product: ai'*x, where i ai denotes the ith row
				// of a
				ax += e.get() * x[e.index()];
			}
			double axb = ax * b[row];
			double thisLoopResult = Math.log(1.0 + Math.exp(-axb));
			result += thisLoopResult;
			row++;
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
		// for (Element e : this.nonZeros.nonZeroes()) {
		// xzuNorm += Math
		// .pow(x[e.index()] - z[e.index()] + u[e.index()], 2.0);
		// }
		double xzuNormScaled = xzuNorm * this.rho / 2.0;
		return xzuNormScaled;
	}

	public int domainDimension() {
		return n;
	}
}