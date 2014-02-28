/* Copyright (C) 2007 Vladimir Roubtsov. All rights reserved.
 */
package com.b5m.bfgs.optimize;

/**
 * Nelder-Mead has a guaranteed cost improvement property for strictly convex
 * functions, however there are no known convergence guarantees otherwise. It works
 * reasonable well for small dimensions (&lt; 10) and for stochastic optimization.<P>
 * <p/>
 * This implementation is a hybrid between NR 10.4 and Bertsekas, p.162-165.
 *
 * @author Vlad Roubtsov, 2007
 */
final class NelderMead<T extends IFunction> extends AbstractOptimizer
        implements IOptimizer<T> {
    // public: ................................................................

    /* (non-Javadoc)
     * @see com.b5m.pig.optimize.IOptimizer#minimize(com.b5m.pig.optimize.IFunction, com.b5m.pig.optimize.IOptimizer.Ctx)
     */
    public final void minimize(final T f, final Ctx ctx) {
        final double[] start = ctx.m_startX;
        final int dimensionality = start.length;
        final int vertex_count = dimensionality + 1;

        final double[] Xcentroid = ctx.m_optimumX; // use caller's own buffer for temporary points
        double[] Xref = new double[dimensionality];
        double[] Xexp = new double[dimensionality];

        final double[][] vertex = new double[vertex_count][]; // vertex point [vertex index][dim index], fully init'ed below
        final double[] Fvertex = new double[vertex_count]; // values of f at the vertex points

        // generate the starting simplex, with the starting point being the first vertex:
        {
            vertex[0] = start.clone();
            ctx.m_startF = Fvertex[0] = f.evaluate(start);

            for (int i = /* !*/1; i <= dimensionality; ++i) {
                double[] temp = start.clone();
                temp[i - 1] += 1.0; // add a unit vector in i'th dim direction

                vertex[i] = temp;
                Fvertex[i] = f.evaluate(temp);
            }
        }
        int evaluationsF = vertex_count;

        final int maxiterations = m_parameters.m_maxIterations;
        final int maxevaluations = m_parameters.m_maxEvaluations;
        final double tolF = m_parameters.m_tolF;

        for (int i = 0; true; ++i) {
            // determine worst, second worst, and best vertex points:
            int v_best = 0;
            int v_worst, v_next_worst;

            if (Fvertex[0] > Fvertex[1]) // maintain invariant i_worst != i_next_worst
            {
                v_worst = 0;
                v_next_worst = 1;
            } else {
                v_worst = 1;
                v_next_worst = 0;
            }

            for (int v = 0; v < vertex_count; ++v) {
                double temp = Fvertex[v];

                if (temp <= Fvertex[v_best]) v_best = v;
                if (temp > Fvertex[v_worst]) {
                    v_next_worst = v_worst;
                    v_worst = v;
                } else if ((v != v_worst) && (temp > Fvertex[v_next_worst])) // i != i_worst guard to maintain invariant at i=1 iteration
                {
                    v_next_worst = v;
                }
            }

            // if the range of function values at the current vertex points relative to midpoint
            // is smaller than tolF, we are done:
            final double Fmin = Fvertex[v_best];
            final double Fmax = Fvertex[v_worst];

            final double[] Xbest = vertex[v_best];

            if ((i >= maxiterations) || (evaluationsF >= maxevaluations)
                    || (2.0 * Math.abs(Fmax - Fmin) / (Math.abs(Fmax) + Math.abs(Fmin) + tolF) < tolF)) {
                ctx.m_optimumF = Fmin;

                final double[] optimum = ctx.m_optimumX;
                for (int d = 0; d < dimensionality; ++d) {
                    optimum[d] = Xbest[d];
                }

                ctx.m_evaluationsF = evaluationsF;
                ctx.m_evaluationsGF = 0;

                break;
            }


            final double[] Xworst = vertex[v_worst];

            // compute centroid of all points except the worst one
            for (int d = 0; d < dimensionality; ++d) {
                double sum = 0.0;
                for (int v = 0; v < vertex_count; ++v) {
                    sum += vertex[v][d];
                }

                Xcentroid[d] = (sum - Xworst[d]) / dimensionality;
            }

            // reflection trial step {Xref = Xcentroid + beta (Xcentroid - Xworst)}:
            for (int d = 0; d < dimensionality; ++d) {
                Xref[d] = (1.0 + BETA) * Xcentroid[d] - BETA * Xworst[d];
            }
            double Fref = f.evaluate(Xref);
            evaluationsF++;


            if (Fref < Fmin) {
                // (1) if Xref is better than the current best, attempt expansion
                // {Xexp = Xref + gamma (Xref - Xcentroid)}:

                for (int d = 0; d < dimensionality; ++d) {
                    Xexp[d] = (1.0 + GAMMA) * Xref[d] - GAMMA * Xcentroid[d];
                }
                final double Fexp = f.evaluate(Xexp);
                evaluationsF++;

                // accept the best of Xref or Xexp:
                if (Fexp < Fref) // replace worst point with the best of Xref and Xexp
                {
                    vertex[v_worst] = Xexp;
                    Xexp = Xworst; // re-use this buffer

                    Fvertex[v_worst] = Fexp;
                } else {
                    vertex[v_worst] = Xref;
                    Xref = Xworst; // re-use this buffer

                    Fvertex[v_worst] = Fref;
                }
            } else if (Fref < Fvertex[v_next_worst]) {
                // (2) if Xref has intermediate cost (better than the second worst),
                // use the reflected point:

                vertex[v_worst] = Xref;
                Xref = Xworst; // re-use this buffer

                Fvertex[v_worst] = Fref;
            } else {
                // (3) Xref is worse that the second worst, perform contraction
                // {Xnew = theta best(Xmax, Xref) + (1-theta) Xcentroid}:

                // reuse Xexp as Xnew:

                if (Fmax <= Fref) // contract towards the best of Xworst and Xref
                {
                    for (int d = 0; d < dimensionality; ++d) {
                        Xexp[d] = THETA * Xworst[d] + (1.0 - THETA) * Xcentroid[d];
                    }
                } else {
                    for (int d = 0; d < dimensionality; ++d) {
                        Xexp[d] = THETA * Xref[d] + (1.0 - THETA) * Xcentroid[d];
                    }
                }
                final double Fcontract = f.evaluate(Xexp);
                evaluationsF++;

                if (Fcontract < Fmax) {
                    vertex[v_worst] = Xexp;
                    Xexp = Xworst; // re-use this buffer

                    Fvertex[v_worst] = Fcontract;
                } else {
                    // (4) this can only happen if 'f' is not strictly convex; as a last resort,
                    // contract the entire vertex towards the best point:

                    for (int v = 0; v < vertex_count; ++v) {
                        if (v != v_best) {
                            final double[] x = vertex[v];
                            for (int d = 0; d < dimensionality; ++d) {
                                x[d] = THETA * x[d] + (1.0 - THETA) * Xbest[d];
                            }

                            Fvertex[v] = f.evaluate(x);
                        }
                    }
                    evaluationsF += dimensionality;
                }
            }

        } // end of iteration for loop
    }

    // protected: .............................................................

    /* (non-Javadoc)
     * @see com.vladium.util.optimize.AbstractOptimizer#defaultParameters()
     */
    @Override
    protected final OptimizerParameters defaultParameters() {
        return DEFAULT_PARAMETERS.clone();
    }

    // package: ...............................................................

    /**
     * Package-protected to enforce Factory pattern.
     */
    NelderMead(final OptimizerParameters parameters) {
        super(parameters);
    }

    // private: ...............................................................

//    private static String dump (final double [][] vertex)
//    {
//        StringWriter s = new StringWriter ();
//        for (int v = 0; v < vertex.length; ++ v)
//        {
//            for (int d = 0; d < vertex [v].length; ++ d)
//            {
//                if (v != 0 || d != 0) s.write (", ");
//                s.write (Double.toString (vertex [v][d]));
//            }
//        }
//        
//        s.flush ();
//        return s.toString ();
//    }

    private static final double BETA = 1.0;
    private static final double GAMMA = 1.0;
    private static final double THETA = 0.5; // smaller means more contraction

    private static final OptimizerParameters DEFAULT_PARAMETERS;

    static {
        final OptimizerParameters parms = new OptimizerParameters();

        parms.m_maxIterations = 500;

        parms.m_maxEvaluations = 200;
        parms.m_tolF = 1.0E-8;

        DEFAULT_PARAMETERS = parms;
    }
}