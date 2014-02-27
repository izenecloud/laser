/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

/**
 * @author Vlad Roubtsov, 2006
 */
public final class GoldenSection implements IOptimizer1V {
    // public: ................................................................

    public final double optimize(final IFunction1V f, double xk, double xkk, final double[] x) {
        // invariant: if 'f' is strictly unimodal, the minimum point is contained in [xk, xkk] 

        double fk_prev = f.evaluate(xk);   // f(xk)
        double fkk_prev = f.evaluate(xkk); // f(xkk)

        double delta = xkk - xk;
        double bk = xk + TAU * delta;
        double bkk = xkk - TAU * delta;

        double fk = f.evaluate(bk);
        double fkk = f.evaluate(bkk);

        int iter = 0;
        while (delta >= EPSILON_X) {
            ++iter;
            if (fk < fkk) {
                // contract the right side of [xk, xkk] interval:

                if (fk_prev <= fk) {
                    // contract all the way to bk:

                    xkk = bk;
                    fkk_prev = fk;

                    // get two new sample points:

                    delta = xkk - xk;
                    bk = xk + TAU * delta;
                    bkk = xkk - TAU * delta;

                    fk = f.evaluate(bk);
                    fkk = f.evaluate(bkk);
                } else {
                    // contract just to bkk:

                    xkk = bkk;
                    fkk_prev = fkk;

                    // bk becomes the next bkk (only need one new sample point):

                    bkk = bk;
                    fkk = fk;

                    delta = xkk - xk;
                    bk = xk + TAU * delta;
                    fk = f.evaluate(bk);
                }
            } else if (fk > fkk) {
                // contract the left side of [xk, xkk] interval:

                if (fkk_prev <= fkk) {
                    // contract all the way to bkk:

                    xk = bkk;
                    fk_prev = fkk;

                    // get two new sample points:

                    delta = xkk - xk;
                    bk = xk + TAU * delta;
                    bkk = xkk - TAU * delta;

                    fk = f.evaluate(bk);
                    fkk = f.evaluate(bkk);
                } else {
                    // contract just to bk:

                    xk = bk;
                    fk_prev = fk;

                    // bkk becomes the next bk (only need one new sample point):

                    bk = bkk;
                    fk = fkk;

                    delta = xkk - xk;
                    bkk = xkk - TAU * delta;
                    fkk = f.evaluate(bkk);
                }
            } else {
                // contract both sides:

                xk = bk;
                fk_prev = fk;

                xkk = bkk;
                fkk_prev = fkk;

                delta = xkk - xk;
                bk = xk + TAU * delta;
                bkk = xkk - TAU * delta;

                fk = f.evaluate(bk);
                fkk = f.evaluate(bkk);
            }
        }

        // we have four sample points, return the best one:

        double bestf, bestx;

        if (fk < fkk) {
            if (fk_prev <= fk) {
                bestf = fk_prev;
                bestx = xk;
            } else {
                bestf = fk;
                bestx = bk;
            }
        } else {
            if (fkk_prev <= fkk) {
                bestf = fkk_prev;
                bestx = xkk;
            } else {
                bestf = fkk;
                bestx = bkk;
            }
        }

        x[0] = bestx;
        return bestf;
    }

    // protected: .............................................................

    // package: ...............................................................

    // private: ...............................................................

    private static final double EPSILON_X = 1.0E-8;
    private static final double TAU = 0.5 * (3.0 - Math.sqrt(5.0));

}