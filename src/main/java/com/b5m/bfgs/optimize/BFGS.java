/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

/**
 * Unconstrained BFGS implementation.
 *
 * @author Vlad Roubtsov, (C) 2005
 */
final public class BFGS<T extends IDifferentiableFunction> extends AbstractOptimizer
        implements IOptimizer<T> {
    // public: ................................................................

    /* (non-Javadoc)
     * @see com.b5m.pig.optimize.IOptimizer#minimize(com.b5m.pig.optimize.IFunction, com.b5m.pig.optimize.IOptimizer.Ctx)
     */
    public final void minimize(final T f, final Ctx ctx) {
        final double[] start = ctx.m_startX;
        final double[] optimum = ctx.m_optimumX;

        final int dimensionality = start.length;
        final IFunctionGradient gradientf = f.gradient();

        final double[] direction = new double[dimensionality]; // x_k+1 = x_k + alpha_k*direction_k

        double[] x = optimum; // use caller's out buffer to represent the current point
        double[] x_prev = (double[]) start.clone();

        double[] Vfx = new double[dimensionality];
        double[] Vfx_prev = new double[dimensionality];

        final double[][] D = new double[dimensionality][dimensionality]; // inverse Hessian approximation

        final double[] pi = new double[dimensionality];  // p_i = x_i+1 - x_i
        final double[] qi = new double[dimensionality];  // q_i = Vfx_i+1 - Vfx_i
        final double[] Dqi = new double[dimensionality]; // Dq_i = |D_i|.q_i:


        double fx = f.evaluate(x_prev); // starting value of f
        ctx.m_startF = fx;
        gradientf.evaluate(x_prev, Vfx_prev); // starting gradient of f

        int evaluationsF = 1, evaluationsGF = 1; // count of function and gradient evaluations

        for (int d = 0; d < dimensionality; ++d) {
            // initialize D to a unit matrix:
            D[d][d] = 1.0;

            // set initial direction to opposite of the starting gradient (since D is just a unit matrix):
            direction[d] = -Vfx_prev[d];
        }

        double delta, temp1, temp2;

        // perform a max of 'maxiterations' of quasi-Newton iteration steps:
        final int maxiterations = m_parameters.m_maxIterations;

        final double tolX = m_parameters.m_tolX;
        final double tolGradient = m_parameters.m_tolGradient;

        for (int i = 0; i < maxiterations; ++i) {
            // do the line search in the current direction:

            fx = LineSearch.search(f, fx, Vfx_prev, x_prev, direction, x); // this updates fx and x

            // if the current point shift (relative to current position) is below tolerance, we're done:
            delta = 0.0;
            for (int d = 0; d < dimensionality; ++d) {
                temp1 = (pi[d] = x[d] - x_prev[d]); // compute p_i = x_i+1 - x_i as a side effect
                temp2 = Math.abs(temp1) / Math.max(Math.abs(x[d]), 1.0);

                if (temp2 > delta) delta = temp2;
            }
            if (delta < tolX)
                break;

            // get the current gradient:  // TODO: use 1 extra fx eval gradient version?
            gradientf.evaluate(x, Vfx); // this updates Vfx
            evaluationsGF++;


            // if the current gradient (normalized by the current x and fx) is below tolerance, we're done:
            delta = 0.0;
            temp1 = Math.max(fx, 1.0);
            for (int d = 0; d < dimensionality; ++d) {
                temp2 = Math.abs(Vfx[d]) * Math.max(Math.abs(x[d]), 1.0) / temp1;

                if (temp2 > delta) delta = temp2;
            }
            if (delta < tolGradient)
                break;

            // compute q_i = Vfx_i+1 - Vfx_i:
            for (int d = 0; d < dimensionality; ++d) {
                qi[d] = Vfx[d] - Vfx_prev[d];
            }

            // compute Dq_i = |D_i|.q_i:
            for (int m = 0; m < dimensionality; ++m) {
                Dqi[m] = 0.0;
                for (int n = 0; n < dimensionality; ++n) {
                    Dqi[m] += D[m][n] * qi[n];
                }
            }

            // compute p_i.q_i and q_i.Dq_i:
            double piqi = 0.0;
            double qiDqi = 0.0;
            double pi_norm = 0.0, qi_norm = 0.0;

            for (int d = 0; d < dimensionality; ++d) {
                temp1 = qi[d];
                temp2 = pi[d];

                piqi += temp2 * temp1;
                qiDqi += temp1 * Dqi[d];

                qi_norm += temp1 * temp1;
                pi_norm += temp2 * temp2;
            }
            // update D using BFGS formula:

            // note that we should not update D when successive pi's are almost
            // linearly dependent; this can be ensured by checking pi.qi = pi|H|pi,
            // which ought to be positive enough if H is positive definite.

            if (piqi > ZERO_PRODUCT * Math.sqrt(qi_norm * pi_norm)) {
                // re-use qi vector to compute v in Bertsekas:
                for (int d = 0; d < dimensionality; ++d) {
                    qi[d] = pi[d] / piqi - Dqi[d] / qiDqi;
                }

                for (int m = 0; m < dimensionality; ++m) {
                    for (int n = m; n < dimensionality; ++n) {
                        D[m][n] += pi[m] * pi[n] / piqi - Dqi[m] * Dqi[n] / qiDqi + qiDqi * qi[m] * qi[n];
                        D[n][m] = D[m][n];
                    }
                }
            }

            // set current direction for the next iteration as -|D|.Vfx 
            for (int m = 0; m < dimensionality; ++m) {
                direction[m] = 0.0;
                for (int n = 0; n < dimensionality; ++n) {
                    direction[m] -= D[m][n] * Vfx[n];
                }
            }

            // update current point and current gradient for the next iteration:
            if (i != maxiterations - 1) // keep the 'x contains the latest point' invariant for the post-loop copy below
            {
                double[] temp = Vfx;
                Vfx = Vfx_prev;
                Vfx_prev = temp;

                temp = x;
                x = x_prev;
                x_prev = temp;
            }
        }

        // copy the final point into ctx (note that x_prev<->x swap hasn't been done):
        if (optimum != x) {
            for (int d = 0; d < dimensionality; ++d) {
                optimum[d] = x[d];
            }
        }

        ctx.m_optimumF = fx;

        ctx.m_evaluationsF = evaluationsF;
        ctx.m_evaluationsGF = evaluationsGF;
    }

    // protected: .............................................................

    /* (non-Javadoc)
     * @see com.b5m.pig.optimize.AbstractOptimizer#defaultParameters()
     */
    @Override
    protected final OptimizerParameters defaultParameters() {
        return DEFAULT_PARAMETERS.clone();
    }

    // package: ...............................................................

    /**
     * Package-protected to enforce Factory pattern.
     */
    public BFGS(final OptimizerParameters parameters) {
        super(parameters);
    }

    // private: ...............................................................

    private static final double ZERO_PRODUCT = 1.0E-8;

    private static final OptimizerParameters DEFAULT_PARAMETERS;

    static {
        final OptimizerParameters parms = new OptimizerParameters();

        parms.m_maxIterations = 200;

        parms.m_tolX = 1.0E-8;
        parms.m_tolGradient = 1.0E-8;

        DEFAULT_PARAMETERS = parms;
    }

}