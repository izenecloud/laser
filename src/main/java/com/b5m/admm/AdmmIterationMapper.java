package com.b5m.admm;
import com.b5m.lbfgs.LogisticL2DiffFunction;

import edu.stanford.nlp.optimization.DiffFunction;
import edu.stanford.nlp.optimization.QNMinimizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.mahout.math.Matrix;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.b5m.admm.AdmmIterationHelper.*;

public class AdmmIterationMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, NullWritable, AdmmReducerContextWritable> {

    private static final Logger LOG = Logger.getLogger(AdmmIterationMapper.class.getName());
    private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
    private static final float DEFAULT_RHO = 0.1f;

    private int iteration;
    private FileSystem fs;
    private Map<String, String> splitToParameters;
    private Set<Integer> columnsToExclude;

    private QNMinimizer lbfgs;
    private boolean addIntercept;
    private float regularizationFactor;
    private double rho;
    private String previousIntermediateOutputLocation;
    private Path previousIntermediateOutputLocationPath;
    private int numFeatures;

    @Override
    public void configure(JobConf job) {
        iteration = Integer.parseInt(job.get("iteration.number"));
        String columnsToExcludeString = job.get("columns.to.exclude");
        columnsToExclude = getColumnsToExclude(columnsToExcludeString);
        addIntercept = job.getBoolean("add.intercept", false);
        rho = job.getFloat("rho", DEFAULT_RHO);
        numFeatures = job.getInt("signal.data.num.features", 0);
        regularizationFactor = job.getFloat("regularization.factor", DEFAULT_REGULARIZATION_FACTOR);
        previousIntermediateOutputLocation = job.get("previous.intermediate.output.location");
        previousIntermediateOutputLocationPath = new Path(previousIntermediateOutputLocation);

        try {
            fs = previousIntermediateOutputLocationPath.getFileSystem(job); //FileSystem.get(job);
        }
        catch (IOException e) {
            LOG.log(Level.FINE, e.toString());
        }

        splitToParameters = getSplitParameters();
        lbfgs = new QNMinimizer();
    }

    protected Map<String, String> getSplitParameters() {
        return readParametersFromHdfs(fs, previousIntermediateOutputLocationPath, iteration);
    }

    public void map(LongWritable key, Text value, OutputCollector<NullWritable, AdmmReducerContextWritable> output, Reporter reporter)
            throws IOException {
        FileSplit split = (FileSplit) reporter.getInputSplit();
        
        String splitId = key.get() + "@" + split.getPath() + ":" + Long.toString(split.getStart()) + " - "+ Long.toString(split.getLength());
        splitId = removeIpFromHdfsFileName(splitId);

        Matrix inputSplitData = createMatrixFromDataString(value.toString(), numFeatures, columnsToExclude, addIntercept);

        AdmmMapperContext mapperContext;
        if (iteration == 0) {
            mapperContext = new AdmmMapperContext(splitId, inputSplitData, rho);
        }
        else {
            mapperContext = assembleMapperContextFromCache(inputSplitData, splitId);
        }
        AdmmReducerContext reducerContext = localMapperOptimization(mapperContext);

        LOG.info("Iteration " + iteration + "Mapper outputting splitId " + splitId);
        output.collect(NullWritable.get(),  new AdmmReducerContextWritable(reducerContext));
    }

    private AdmmReducerContext localMapperOptimization(AdmmMapperContext context) {
    	LogisticL2DiffFunction myFunction =
                new LogisticL2DiffFunction(context.getA(),
                        context.getB(),
                        context.getRho(),
                        context.getUInitial(),
                        context.getZInitial());
        Ctx optimizationContext = new Ctx(context.getXInitial());
       
        double[] optimum = lbfgs.minimize((DiffFunction)myFunction, 1e-10, context.getXInitial());
        for (int d = 0; d < optimum.length ; ++d) {
        	optimizationContext.m_optimumX[d] = optimum[d];
        }
        double primalObjectiveValue = myFunction.evaluatePrimalObjective(optimizationContext.m_optimumX);
        return new AdmmReducerContext(
        		context.getSplitId(),
        		context.getUInitial(),
                context.getXInitial(),
                optimizationContext.m_optimumX,
                context.getZInitial(),
                primalObjectiveValue,
                context.getRho(),
                regularizationFactor);
    }

    private AdmmMapperContext assembleMapperContextFromCache(Matrix inputSplitData, String splitId) throws IOException {
        if (splitToParameters.containsKey(splitId)) {
            AdmmMapperContext preContext = jsonToAdmmMapperContext(splitToParameters.get(splitId));
            return new AdmmMapperContext(splitId, 
            		inputSplitData,
                    preContext.getUInitial(),
                    preContext.getXInitial(),
                    preContext.getZInitial(),
                    preContext.getRho(),
                    preContext.getLambdaValue(),
                    preContext.getPrimalObjectiveValue(),
                    preContext.getRNorm(),
                    preContext.getSNorm());
        }
        else {
            LOG.log(Level.FINE, "Key not found. Split ID: " + splitId + " Split Map: " + splitToParameters.toString());
            throw new IOException("Key not found.  Split ID: " + splitId + " Split Map: " + splitToParameters.toString());
        }
    }
}