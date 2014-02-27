/* Copyright (C) 2007 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

/**
 * @author Vlad Roubtsov, 2007
 */
abstract class AbstractOptimizer {
    // public: ................................................................

    // protected: .............................................................

    protected final OptimizerParameters m_parameters;

    protected AbstractOptimizer(final OptimizerParameters parameters) {
        final OptimizerParameters combined = defaultParameters().clone();
        combined.combine(parameters);

        m_parameters = combined;
    }

    protected abstract OptimizerParameters defaultParameters();

    // package: ...............................................................

    // private: ...............................................................

}