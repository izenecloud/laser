/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author Vlad Roubtsov, (C) 2005
 */
public interface IOptimizer<T extends IFunction> {
    /**
     * Unconstrained optimization of a given function. The implementation does
     * not rescale f's inputs (the effective scale is 1.0) -- it is the responsibility
     * of the caller to define 'f' via the appropriately scaled variables. Similar for
     * 'f' itself.
     *
     * @param f
     * @param ctx
     */
    void minimize(T f, Ctx ctx);

    /**
     * Context for optimization results and tracing.
     */
    public static final class Ctx {
        public final double[] m_startX;
        public final double[] m_optimumX;

        public double m_startF;
        public double m_optimumF;

        public int m_evaluationsF;
        public int m_evaluationsGF;

        public Ctx(final double[] startX) {
            if (startX == null)
                throw new IllegalArgumentException("null input: startX");

            m_startX = startX.clone();
            m_optimumX = new double[m_startX.length];

            m_startF = Double.NaN;
            m_optimumF = Double.NaN;
        }

        // Object:

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            final StringWriter out = new StringWriter();
            final PrintWriter pout = new PrintWriter(out);

            pout.println("Fopt = " + m_optimumF + " (Fstart = " + m_startF + ")");
            pout.println("Xopt:");
            pout.println("{");
            for (int i = 0; i < m_optimumX.length; ++i) {
                pout.println("  [" + i + "]: " + m_optimumX[i]);
            }
            pout.println("}");
            pout.println("F calls: " + m_evaluationsF + ", G calls: " + m_evaluationsGF);

            pout.flush();
            return out.toString();
        }

    } // end of nested class


    abstract class Factory {
        /**
         * Corresponds to <code>create (type, null)</code>.
         */
        public static <CT extends IFunction> IOptimizer<CT> create(final Class<CT> type) {
            return create(type, null);
        }

        /**
         * @param type       class of functions to be optimized [may not be null]
         * @param parameters [null will cause parameters to default to algorithm-specific defaults]
         * @return a new instance of optimizer, configured with given parameters
         */
        public static <CT extends IFunction> IOptimizer<CT> create(final Class<CT> type, final OptimizerParameters parameters) {
            if (type == null)
                throw new IllegalArgumentException("null input: type");

            if (type == IDifferentiableFunction.class)
                return (IOptimizer<CT>) new BFGS<IDifferentiableFunction>(parameters);
            else
                return new NelderMead<CT>(parameters);
        }

    } // end of nested class

}