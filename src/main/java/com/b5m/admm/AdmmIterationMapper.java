package com.b5m.admm;

import com.b5m.bfgs.optimize.BFGS;
import com.b5m.bfgs.optimize.IOptimizer;
import com.b5m.bfgs.optimize.LogisticL2DifferentiableFunction;
import com.b5m.bfgs.optimize.OptimizerParameters;
import com.b5m.lbfgs.LogisticL2DiffFunction;

import edu.stanford.nlp.optimization.DiffFunction;
import edu.stanford.nlp.optimization.QNMinimizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.b5m.admm.AdmmIterationHelper.*;

public class AdmmIterationMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, NullWritable, Text> {

	//TODO remove?
    private static final Logger LOG = Logger.getLogger(AdmmIterationMapper.class.getName());
    private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
    private static final float DEFAULT_RHO = 0.1f;

    private int iteration;
    private FileSystem fs;
    private Map<String, String> splitToParameters;
    private Set<Integer> columnsToExclude;

    private OptimizerParameters optimizerParameters = new OptimizerParameters();
    private QNMinimizer lbfgs = new QNMinimizer();
//    private BFGS<LogisticL2DifferentiableFunction> bfgs = new BFGS<LogisticL2DifferentiableFunction>(optimizerParameters);
    private boolean addIntercept;
    private float regularizationFactor;
    private double rho;
    private String previousIntermediateOutputLocation;
    private Path previousIntermediateOutputLocationPath;

    @Override
    public void configure(JobConf job) {
        iteration = Integer.parseInt(job.get("iteration.number"));
        String columnsToExcludeString = job.get("columns.to.exclude");
        columnsToExclude = getColumnsToExclude(columnsToExcludeString);
        addIntercept = job.getBoolean("add.intercept", false);
        rho = job.getFloat("rho", DEFAULT_RHO);
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
    }

    protected Map<String, String> getSplitParameters() {
        return readParametersFromHdfs(fs, previousIntermediateOutputLocationPath, iteration);
    }

    public void map(LongWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
            throws IOException {
        FileSplit split = (FileSplit) reporter.getInputSplit();
        String splitId = key.get() + "@" + split.getPath();
        splitId = removeIpFromHdfsFileName(splitId);

        double[][] inputSplitData = createMatrixFromDataString(value.toString(), columnsToExclude, addIntercept);

        AdmmMapperContext mapperContext;
        if (iteration == 0) {
            mapperContext = new AdmmMapperContext(inputSplitData, rho);
        }
        else {
            mapperContext = assembleMapperContextFromCache(inputSplitData, splitId);
        }
        AdmmReducerContext reducerContext = localMapperOptimization(mapperContext);

        LOG.info("Iteration " + iteration + "Mapper outputting splitId " + splitId);
        output.collect(NullWritable.get(), new Text(splitId + "::" + admmReducerContextToJson(reducerContext)));
    }

    private AdmmReducerContext localMapperOptimization(AdmmMapperContext context) {
        /*LogisticL2DifferentiableFunction myFunction =
                new LogisticL2DifferentiableFunction(context.getA(),
                        context.getB(),
                        context.getRho(),
                        context.getUInitial(),
                        context.getZInitial());*/
    	LogisticL2DiffFunction myFunction =
                new LogisticL2DiffFunction(context.getA(),
                        context.getB(),
                        context.getRho(),
                        context.getUInitial(),
                        context.getZInitial());
        IOptimizer.Ctx optimizationContext = new IOptimizer.Ctx(context.getXInitial());
        lbfgs.minimize((DiffFunction)myFunction, 1e-10, context.getXInitial());
        //bfgs.minimize(myFunction, optimizationContext);
        double primalObjectiveValue = myFunction.evaluatePrimalObjective(optimizationContext.m_optimumX);
        return new AdmmReducerContext(context.getUInitial(),
                context.getXInitial(),
                optimizationContext.m_optimumX,
                context.getZInitial(),
                primalObjectiveValue,
                context.getRho(),
                regularizationFactor);
    }

    private AdmmMapperContext assembleMapperContextFromCache(double[][] inputSplitData, String splitId) throws IOException {
        if (splitToParameters.containsKey(splitId)) {
            AdmmMapperContext preContext = jsonToAdmmMapperContext(splitToParameters.get(splitId));
            return new AdmmMapperContext(inputSplitData,
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