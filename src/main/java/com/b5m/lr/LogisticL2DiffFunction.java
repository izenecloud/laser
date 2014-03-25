package com.b5m.lr;

import java.util.Iterator;
import java.util.List;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.nlp.optimization.DiffFunction;

public class LogisticL2DiffFunction implements DiffFunction {
	private static final Logger LOG = LoggerFactory
			.getLogger(LogisticL2DiffFunction.class);
	private final List<Vector> a;
//	private final Vector nonZero;
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
//		nonZero = new SequentialAccessSparseVector(n);
//		Iterator<Vector> iterator = this.a.iterator();
//		while (iterator.hasNext()) {
//			for (Element e : iterator.next().nonZeroes()) {
//				nonZero.setQuick(e.index(), 1);
//			}
//		}
	}

	public double valueAt(double[] x) {
		double result = 0.0;
		// 1+ exp(-b'AX)
//		long sTime = System.nanoTime();
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
		// TODO
		double penalty = 0.0;
//		for (Element e : nonZero.nonZeroes()) {
//			penalty += x[e.index()] * x[e.index()];
//		}
		for (int i = 0; i < n; i++) {
			penalty += x[i] * x[i];
		}
	
		result += penalty * this.lambda / 2;
//		long eTime = System.nanoTime();
//		LOG.info("Time for Value Function, evaluate time = {}", eTime - sTime);
		return result;
	}

	public int domainDimension() {
		return n;
	}

	public double[] derivativeAt(double[] x) {
		// -A'b / (1 + exp(b'Ax) + 2 lambda *x
//		long sTime = System.nanoTime();

		double[] out = new double[n];
//		for (Element e : nonZero.nonZeroes()) {
//			out[e.index()] = 0.0;
//		}
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
		
		
//		for (int i = 0; i < x.length; i++) {
//			out[i] /= this.m;
//		}
		// l2 regularization
//		for (Element e : nonZero.nonZeroes()) {
//			int i = e.index();
//			out[i] /= this.m;
//			out[i] += this.lambda * x[i];
//		}
		for (int i = 0; i < n; i++) {
			out[i] /= this.m;
			out[i] += this.lambda * x[i];
		}

//		long eTime = System.nanoTime();
//		LOG.info("Time for Derivative Function, evaluate time = {}", eTime - sTime);
		return out;
	}

}
