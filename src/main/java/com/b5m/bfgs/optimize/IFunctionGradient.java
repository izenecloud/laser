/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

/**
 * @author Vlad Roubtsov
 */
public interface IFunctionGradient {
    void evaluate(double[] x, double[] out);

}