/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

/**
 * @author Vlad Roubtsov
 */
public
abstract class LineSearch {
    // public: ................................................................

    public static double search(final IFunction f,
                                final double fx, final double[] Vfx, final double[] x,
                                double[] direction, final double[] xout) {
        // [Armijo rule]

        direction = (double[]) direction.clone();

        final int dLimit = x.length;

        // compute direction normalizer:

        double dnorm = 0.0;
        for (int d = 0; d < dLimit; ++d) {
            final double t = direction[d];
            dnorm += t * t;
        }
        dnorm = Math.sqrt(dnorm);

        if (dnorm <= ZERO)
            throw new IllegalArgumentException("'direction' is a zero vector");

        // normalize direction (to avoid making the initial step too big):

        for (int d = 0; d < dLimit; ++d) {
            direction[d] /= dnorm;
        }

        // compute Vfx * direction (normalized):

        double p = 0.0;
        for (int d = 0; d < dLimit; ++d) {
            p += Vfx[d] * direction[d];
        }
        if (p >= 0.0)
            throw new IllegalArgumentException("'direction' is not a descent direction [p = " + p + "]");


        double alpha = 1.0; // initial step size

        for (int i = 0; ; ++i) {
            // take the step:

            for (int d = 0; d < dLimit; ++d) {
                xout[d] = x[d] + alpha * direction[d];
            }

            final double fx_alpha = f.evaluate(xout);

            if (fx_alpha < fx + SIGMA * alpha * p)
                return fx_alpha;
            else {
                if (i == 0) {
                    // first step: do quadratic approximation along the direction
                    // line and set alpha to be the minimizer of that approximation:

                    alpha = 0.5 * p / (p + fx - fx_alpha);
                } else {
                    alpha *= BETA; // reduce the step
                }
            }

            if (alpha < ZERO) // prevent alpha from becoming too small
            {
                if (fx_alpha > fx) {
                    for (int d = 0; d < dLimit; ++d) {
                        xout[d] = x[d];
                    }

                    return fx;
                } else {
                    return fx_alpha;
                }
            }
        }
    }

    // protected: .............................................................

    // package: ...............................................................

    // private: ...............................................................

    private static final double SIGMA = 1.0E-4;
    private static final double BETA = 0.5;

    private static final double ZERO = 1.0E-10;

}