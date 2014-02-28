package com.b5m.admm;

import java.io.PrintWriter;
import java.io.StringWriter;
/**
 * Context for optimization results and tracing.
 */
public  class Ctx {
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
