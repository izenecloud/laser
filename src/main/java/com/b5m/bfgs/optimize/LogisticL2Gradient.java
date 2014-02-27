package com.b5m.bfgs.optimize;

import com.b5m.bfgs.optimize.IFunctionGradient;

public class LogisticL2Gradient implements IFunctionGradient {
    private double[][] a;
    private double[] b;
    private double[] u;
    private double[] z;
    private double rho;
    private int m;
    private int n;

    public LogisticL2Gradient(double[][] a, double[] b, double rho, double[] u, double[] z) {
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

    /*
        @param x: input, x vector value
        @param out: output, with memory pre-allocated (as in BFGS.java line 33), dimension = this.n
     */
    public void evaluate(double[] x, double[] out) {
        for (int vectorIndex = 0; vectorIndex < this.n; vectorIndex++) {
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
    }
}