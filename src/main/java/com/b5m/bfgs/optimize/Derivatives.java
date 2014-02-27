/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

/**
 * @author Vlad Roubtsov
 */
public
abstract class Derivatives {
    // public: ................................................................

    // one function evaluation
    public static void gradient(final IFunction f, final double fx, final double[] x, final double[] out) {
        final double[] xh = (double[]) x.clone();

        for (int d = 0, dLimit = x.length; d < dLimit; ++d) {
            final double old_x_d = xh[d];

            double h;

            // set step h using x[d] as f scale factor along d-th dimension:
            if (Math.abs(old_x_d) < ZERO)
                h = SQRT_EPSILON;
            else
                h = SQRT_EPSILON * old_x_d;

            // convert h to a representable number:
            h = Double.longBitsToDouble(Double.doubleToLongBits(h));

            xh[d] = old_x_d + h;
            final double fxh = f.evaluate(xh);
            xh[d] = old_x_d;

            out[d] = (fxh - fx) / h;
        }
    }

    // two function evaluations
    public static void gradient(final IFunction f, final double[] x, final double[] out) {
        final double[] xh = (double[]) x.clone();

        for (int d = 0, dLimit = x.length; d < dLimit; ++d) {
            final double old_x_d = xh[d];

            double h;

            // set step h using x[d] as f scale factor along d-th dimension:
            if (Math.abs(old_x_d) < ZERO)
                h = SQRT_EPSILON;
            else
                h = SQRT_EPSILON * old_x_d;

            // convert h to a representable number:
            h = Double.longBitsToDouble(Double.doubleToLongBits(h));

            xh[d] = old_x_d + h;
            final double fxh1 = f.evaluate(xh);
            xh[d] = old_x_d - h;
            final double fxh2 = f.evaluate(xh);
            xh[d] = old_x_d;

            out[d] = 0.5 * (fxh1 - fxh2) / h;
        }
    }

    // protected: .............................................................

    // package: ...............................................................

    // private: ...............................................................


    private static final double EPSILON = 1.0E-15;
    private static final double SQRT_EPSILON = Math.sqrt(EPSILON);

    private static final double ZERO = 1.0E-30;

}