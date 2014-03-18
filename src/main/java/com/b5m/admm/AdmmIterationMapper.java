package com.b5m.admm;

import com.b5m.lbfgs.LogisticL2DiffFunction;

import edu.stanford.nlp.optimization.DiffFunction;
import edu.stanford.nlp.optimization.QNMinimizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.b5m.admm.AdmmIterationHelper.*;

public class AdmmIterationMapper
		extends
		Mapper<Writable, VectorWritable, NullWritable, AdmmReducerContextWritable> {

	private static final Logger LOG = Logger
			.getLogger(AdmmIterationMapper.class.getName());
	private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
	private static final float DEFAULT_RHO = 0.1f;

	private int iteration;
	private FileSystem fs;
	private Map<String, String> splitToParameters;
	// private Set<Integer> columnsToExclude;

	private QNMinimizer lbfgs;
	private boolean addIntercept;
	private float regularizationFactor;
	private double rho;
	private String previousIntermediateOutputLocation;
	private Path previousIntermediateOutputLocationPath;
	private Matrix inputSplitData;
	private String splitId;
	private List<Vector> input;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		iteration = Integer.parseInt(conf.get("iteration.number"));
		String columnsToExcludeString = conf.get("columns.to.exclude");
		// columnsToExclude = getColumnsToExclude(columnsToExcludeString);
		addIntercept = conf.getBoolean("add.intercept", false);
		rho = conf.getFloat("rho", DEFAULT_RHO);
		regularizationFactor = conf.getFloat("regularization.factor",
				DEFAULT_REGULARIZATION_FACTOR);
		previousIntermediateOutputLocation = conf
				.get("previous.intermediate.output.location");
		previousIntermediateOutputLocationPath = new Path(
				previousIntermediateOutputLocation);

		try {
			fs = previousIntermediateOutputLocationPath.getFileSystem(conf); // FileSystem.get(job);
		} catch (IOException e) {
			LOG.log(Level.FINE, e.toString());
		}

		splitToParameters = getSplitParameters();
		lbfgs = new QNMinimizer();

		FileSplit split = (FileSplit) context.getInputSplit();
		splitId = split.getPath() + ":" + Long.toString(split.getStart())
				+ " - " + Long.toString(split.getLength());
		splitId = removeIpFromHdfsFileName(splitId);

		input = new LinkedList<Vector>();
	}

	protected void map(Writable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		input.add(value.get());
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		inputSplitData = new SparseRowMatrix(input.size(), input.get(0).size());
		for (int i = 0; i < input.size(); i++) {
			Vector v = input.get(i);
			if (addIntercept) {
				v.set(0, 1);
			}
			inputSplitData.viewRow(i).assign(v);
			v = null;
		}
		input = null;

		AdmmMapperContext mapperContext;
		if (iteration == 0) {
			mapperContext = new AdmmMapperContext(splitId, inputSplitData, rho);
		} else {
			mapperContext = assembleMapperContextFromCache(inputSplitData,
					splitId);
		}
		AdmmReducerContext reducerContext = localMapperOptimization(mapperContext);

		LOG.info("Iteration " + iteration + "Mapper outputting splitId "
				+ splitId);
		context.write(NullWritable.get(), new AdmmReducerContextWritable(
				reducerContext));
	}

	protected Map<String, String> getSplitParameters() {
		return readParametersFromHdfs(fs,
				previousIntermediateOutputLocationPath, iteration);
	}

	private AdmmReducerContext localMapperOptimization(AdmmMapperContext context) {
		LogisticL2DiffFunction myFunction = new LogisticL2DiffFunction(
				context.getA(), context.getB(), context.getRho(),
				context.getUInitial(), context.getZInitial());
		Ctx optimizationContext = new Ctx(context.getXInitial());

		double[] optimum = lbfgs.minimize((DiffFunction) myFunction, 1e-10,
				context.getXInitial());
		for (int d = 0; d < optimum.length; ++d) {
			optimizationContext.m_optimumX[d] = optimum[d];
		}
		double primalObjectiveValue = myFunction
				.evaluatePrimalObjective(optimizationContext.m_optimumX);
		return new AdmmReducerContext(context.getSplitId(),
				context.getUInitial(), context.getXInitial(),
				optimizationContext.m_optimumX, context.getZInitial(),
				primalObjectiveValue, context.getRho(), regularizationFactor);
	}

	private AdmmMapperContext assembleMapperContextFromCache(
			Matrix inputSplitData, String splitId) throws IOException {
		if (splitToParameters.containsKey(splitId)) {
			AdmmMapperContext preContext = jsonToAdmmMapperContext(splitToParameters
					.get(splitId));
			return new AdmmMapperContext(splitId, inputSplitData,
					preContext.getUInitial(), preContext.getXInitial(),
					preContext.getZInitial(), preContext.getRho(),
					preContext.getLambdaValue(),
					preContext.getPrimalObjectiveValue(),
					preContext.getRNorm(), preContext.getSNorm());
		} else {
			LOG.log(Level.FINE, "Key not found. Split ID: " + splitId
					+ " Split Map: " + splitToParameters.toString());
			throw new IOException("Key not found.  Split ID: " + splitId
					+ " Split Map: " + splitToParameters.toString());
		}
	}
}