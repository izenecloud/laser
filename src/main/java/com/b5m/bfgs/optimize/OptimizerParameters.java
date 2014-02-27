/* Copyright (C) 2007 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

/**
 * A mutable class for specifying control parameters for optimization algorithms.
 * The class is kept mutable to make it more convenient to add and override
 * algorithm-specific settings without definint a large set of constructions.
 *
 * @author Vlad Roubtsov, 2007
 */
public
final class OptimizerParameters implements Cloneable {
    // public: ................................................................

    public OptimizerParameters() {
        m_maxIterations = -1;
        m_maxEvaluations = -1;

        m_tolX = Double.NaN;
        m_tolGradient = Double.NaN;

        m_tolF = Double.NaN;
    }

    /**
     * Maximum number of iterations.
     */
    public int m_maxIterations;

    // BFGS: two stopping criteria (note: BFGS does not limit by function evaluations)

    /**
     * BFGS stops if the relative change in the current position x (max across all dimensions) is
     * smaller than this tolerance.
     */
    public double m_tolX;
    /**
     * BFGS stops if the current gradient, normalized by the current x and function value, is
     * smaller than this tolerance.
     */
    public double m_tolGradient;

    // Nelder-Mead: two stopping criteria

    /**
     * Nelder-Mead stops if the function value could not be decreased by more than m_tolF * (abs
     */
    public double m_tolF;

    /**
     * Nelder-Mead stops if more than maximum number of function calls has been made (note that
     * because an iteration can make more than 1 function call this bound may be overshot).
     */
    public int m_maxEvaluations;

    // Object:

    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    @Override
    public OptimizerParameters clone() {
        try {
            return (OptimizerParameters) super.clone();
        } catch (CloneNotSupportedException cnse) {
            throw new InternalError(cnse.toString());
        }
    }

    // protected: .............................................................

    // package: ...............................................................

    void combine(final OptimizerParameters overrides) {
        if (overrides == null) return;

        if (overrides.m_maxIterations > 0) m_maxIterations = overrides.m_maxIterations;

        if (!Double.isNaN(overrides.m_tolX)) m_tolX = overrides.m_tolX;
        if (!Double.isNaN(overrides.m_tolGradient)) m_tolGradient = overrides.m_tolGradient;

        if (!Double.isNaN(overrides.m_tolF)) m_tolF = overrides.m_tolF;
        if (overrides.m_maxEvaluations > 0) m_maxEvaluations = overrides.m_maxEvaluations;
    }

    // private: ...............................................................

}