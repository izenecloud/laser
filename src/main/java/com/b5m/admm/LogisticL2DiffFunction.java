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
 * f(x) = (1 / m) * log[1 + exp(-b'Ax)] + rho / 2 * (x - z + u)^2
 * df/dx = (1 / m) * (-A'b) / [1 + exp(b'Ax)] + rho * (x -z + u)
 */

public class LogisticL2DiffFunction implements DiffFunction {
	private static final Logger LOG = LoggerFactory
			.getLogger(LogisticL2DiffFunction.class.getName());
	private double[] ab; // n by 1 vector of A'b
	private double[] u;
	private double[] z;
	// private Vector nonZeros;
	private double rho;
	private int m; // number of samples
	private int n; // number of features (assumed that feature 0 is the
					// intercept)

	public LogisticL2DiffFunction(List<Vector> a, double[] b, double rho,
			double[] u, double[] z) {
		long sTime = System.nanoTime();

		LOG.info("Initialize LogisticL2DiffFunction");
		this.rho = rho;
		this.m = a.size();
		if (this.m > 0) {
			this.n = a.get(0).size() - 1;
		} else {
			this.n = 0;
		}

		this.u = u;
		this.z = z;
		this.ab = new double[this.n];
		Iterator<Vector> iterator = a.iterator();
		int row = 0;
		while (iterator.hasNext()) {
			Vector v = iterator.next();
			for (Element e : v.nonZeroes()) {
				int i = e.index();
				this.ab[i] += e.get() * b[row];
			}
			row++;
		}
		LOG.info("Initialize LogisticL2DiffFunction Finish, Time: = {}",
				System.nanoTime() - sTime);

	}

	public double[] derivativeAt(double[] x) {
		long sTime = System.nanoTime();
		// A'b
		double[] out = new double[x.length];
		double bax = 0.0;
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] = this.ab[vectorIndex];
			bax += x[vectorIndex] * this.ab[vectorIndex];
		}

		// 1+ exp(b'A)X
		double denominator = 1.0 + Math.exp(bax);
		denominator *= -1 * this.m;
		for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
			out[vectorIndex] /= denominator;
		}

		// penalty
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
		double bax = 0.0;
		for (int i = 0; i < x.length; i++) {
			bax += this.ab[i] * x[i];
		}
		double result = Math.log(1 + Math.exp(-bax));
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