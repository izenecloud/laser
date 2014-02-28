package com.b5m.lbfgs;

import edu.stanford.nlp.optimization.DiffFunction;


public class LogisticL2DiffFunction implements DiffFunction {
    private double[][] a; // m by n matrix of features
    private double[] b; // m by 1 vector of labels
    private double[] u;
    private double[] z;
    private double rho;
    private int m; // number of samples
    private int n; // number of features (assumed that feature 0 is the intercept)

    public LogisticL2DiffFunction(double[][] a, double[] b, double rho, double[] u, double[] z) {
        this.a = a;
        this.b = b;
        this.rho = rho;
        this.m = a.length;
        if (this.m > 0) {
            this.n = a[0].length;
        } else {
            this.n = 0;
        }
        this.u = u;
        this.z = z;
    }
    public double[] derivativeAt(double[] x) {
    	double [] out = new double[x.length];
    	for (int vectorIndex = 0; vectorIndex < x.length; vectorIndex++) {
            out[vectorIndex] = 0.0; // reset result values to 0.0
        }
        for (int row = 0; row < this.m; row++) {
            double ax = 0.0;
            for (int col = 0; col < this.n; col++) {
                ax += this.a[row][col] * x[col];
            }
            double thisRowMultiplier = this.b[row] / (1.0 + Math.exp(this.b[row] * ax));
            for (int col = 0; col < this.n; col++) {
                out[col] += -this.a[row][col] * thisRowMultiplier;
            }
        }
        for (int col = 0; col < this.n; col++) {
            out[col] /= this.m;
        }
        for (int col = 0; col < this.n; col++) {
            out[col] += this.rho * (x[col] - this.z[col] + this.u[col]);
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
            for (int col = 0; col < n; col++) {
                // Calculate dot product: ai'*x, where i ai denotes the ith row of a
                ax += this.a[row][col] * x[col];
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
        for (int col = 0; col < n; col++) {
            xzuNorm += Math.pow(x[col] - z[col] + u[col], 2.0);
        }
        double xzuNormScaled = xzuNorm * this.rho / 2.0;
        return xzuNormScaled;
    }
    
	public int domainDimension() {
		return n;
	}
}