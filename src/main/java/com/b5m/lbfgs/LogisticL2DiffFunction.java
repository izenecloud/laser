package com.b5m.lbfgs;

import java.util.logging.Logger;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import edu.stanford.nlp.optimization.DiffFunction;


public class LogisticL2DiffFunction implements DiffFunction {
    private static final Logger LOG = Logger.getLogger(LogisticL2DiffFunction.class.getName());

    private Matrix a; // m by n matrix of features
    private double[] b; // m by 1 vector of labels
    private double[] u;
    private double[] z;
    private Vector nonZeros;
    private double rho;
    private int m; // number of samples
    private int n; // number of features (assumed that feature 0 is the intercept)

    public LogisticL2DiffFunction(Matrix a, double[] b, double rho, double[] u, double[] z) {
        this.a = a;
        this.b = b;
        this.rho = rho;
        this.m = a.numRows();
        if (this.m > 0) {
            this.n = a.numCols();
        } else {
            this.n = 0;
        }
        this.u = u;
        this.z = z;
        this.nonZeros = new SequentialAccessSparseVector(a.numCols());
        for (int row = 0; row < this.m; row++) {       
            for (Element e : this.a.viewRow(row).nonZeroes()) {
            	this.nonZeros.set(e.index(), e.get());
            }
        }
    }
    public double[] derivativeAt(double[] x) {
    	double [] out = new double[x.length];
    	for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
            out[vectorIndex] = 0.0; // reset result values to 0.0
        }
        for (int row = 0; row < this.m; row++) {
            double ax = 0.0;
        
            for (Element e : this.a.viewRow(row).nonZeroes()) {
            	ax += e.get() * x[e.index()];
            }
            //for (int col = 0; col < this.n; col++) {
            //    ax += this.a.getQuick(row, col) * x[col];
            //}
            double thisRowMultiplier = this.b[row] / (1.0 + Math.exp(this.b[row] * ax));
            for (Element e : this.a.viewRow(row).nonZeroes()) {
            	out[e.index()] += -e.get() * thisRowMultiplier;
            }
            //for (int col = 0; col < this.n; col++) {
            //    out[col] += -this.a.getQuick(row, col) * thisRowMultiplier;
            //}
        }
        for (Element e : this.a.viewRow(0).nonZeroes()){
        //for (int col = 0; col < this.n; col++) {
            out[e.index()] /= this.m;
        }
        for (Element e : this.a.viewRow(0).nonZeroes()){
        //for (int col = 0; col < this.n; col++) {
            out[e.index()] += this.rho * (x[e.index()] - this.z[e.index()] + this.u[e.index()]);
        }
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
        for (int row = 0; row < m; row++) {
            double ax = 0;
            for (Element e : this.a.viewRow(row).nonZeroes()) {
            //for (int col = 0; col < n; col++) {
                // Calculate dot product: ai'*x, where i ai denotes the ith row of a
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
        //TODO All non-zeroes in A
        for (Element e : this.nonZeros.nonZeroes()) {
            xzuNorm += Math.pow(x[e.index()] - z[e.index()] + u[e.index()], 2.0);
        //for (int col = 0; col < n; col++) {
        //    xzuNorm += Math.pow(x[col] - z[col] + u[col], 2.0);
        }
        double xzuNormScaled = xzuNorm * this.rho / 2.0;
        return xzuNormScaled;
    }
    
	public int domainDimension() {
		return n;
	}
}