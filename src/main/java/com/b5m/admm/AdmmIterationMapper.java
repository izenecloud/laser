package com.b5m.admm;

import edu.stanford.nlp.optimization.DiffFunction;
import edu.stanford.nlp.optimization.QNMinimizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.b5m.admm.AdmmIterationHelper.*;

public class AdmmIterationMapper
		extends
		Mapper<Writable, VectorWritable, NullWritable, AdmmReducerContextWritable> {

	public static final Logger LOG = LoggerFactory
			.getLogger(AdmmIterationMapper.class.getName());
	private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
	private static final float DEFAULT_RHO = 0.1f;

	private int iteration;
	private FileSystem fs;

	private QNMinimizer lbfgs;
	private boolean addIntercept;
	private float regularizationFactor;
	private double rho;
	private String previousIntermediateOutputLocation;
	private Path previousIntermediateOutputLocationPath;
	private String splitId;
	private List<Vector> inputSplitData;

	private Configuration conf;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
		iteration = Integer.parseInt(conf.get("iteration.number"));
		addIntercept = conf.getBoolean("add.intercept", false);
		rho = conf.getFloat("rho", DEFAULT_RHO);
		regularizationFactor = conf.getFloat("regularization.factor",
				DEFAULT_REGULARIZATION_FACTOR);
		previousIntermediateOutputLocation = conf
				.get("previous.intermediate.output.location");
		previousIntermediateOutputLocationPath = new Path(
				previousIntermediateOutputLocation);

		try {
			fs = previousIntermediateOutputLocationPath.getFileSystem(conf);
		} catch (IOException e) {
			LOG.info(e.toString());
		}

		lbfgs = new QNMinimizer();

		FileSplit split = (FileSplit) context.getInputSplit();
		splitId = split.getPath() + ":" + Long.toString(split.getStart())
				+ " - " + Long.toString(split.getLength());
		splitId = removeIpFromHdfsFileName(splitId);

		inputSplitData = new LinkedList<Vector>();
	}

	protected void map(Writable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		Vector v = value.get();
		if (addIntercept) {
			v.set(0, 1.0);
		}
		inputSplitData.add(v);
	}

	@SuppressWarnings("unchecked")
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		//TODO
		LOG.info("Input Split Size : row = {}, col = {}",
				inputSplitData.size(), inputSplitData.get(0).size());
		Vector[] vecArray = new Vector[inputSplitData.size()];

		Iterator<Vector> iterator = inputSplitData.iterator();
		int row = 0;
		while (iterator.hasNext()) {
			vecArray[row] = iterator.next();
			row++;
		}

		AdmmMapperContext mapperContext;
		if (iteration == 0) {
			mapperContext = new AdmmMapperContext(splitId, vecArray, rho);
		} else {
			mapperContext = assembleMapperContextFromCache(vecArray, splitId);
		}
		AdmmReducerContext reducerContext = localMapperOptimization(mapperContext);

		LOG.info("Iteration " + iteration + "Mapper outputting splitId "
				+ splitId);

		context.write(NullWritable.get(), new AdmmReducerContextWritable(
				reducerContext));

		Configuration conf = context.getConfiguration();

		RecordWriter<NullWritable, DoubleArrayWritable> writer = null;
		try {
			conf.setClass("com.b5m.admm.iteration.output.class",
					DoubleArrayWritable.class, Writable.class);
			conf.set("com.b5m.admm.iteration.output.name", "X-" + this.splitId);

			writer = (RecordWriter<NullWritable, DoubleArrayWritable>) context
					.getOutputFormatClass().newInstance()
					.getRecordWriter(context);
			writer.write(NullWritable.get(), new DoubleArrayWritable(
					reducerContext.getXUpdated()));
			writer.close(context);

			conf.set("com.b5m.admm.iteration.output.name", "U-" + this.splitId);
			writer = (RecordWriter<NullWritable, DoubleArrayWritable>) context
					.getOutputFormatClass().newInstance()
					.getRecordWriter(context);
			writer.write(NullWritable.get(), new DoubleArrayWritable(
					reducerContext.getUInitial()));
			writer.close(context);

		} catch (Exception e) {
			LOG.error(e.getMessage());
			throw new IOException(e.getMessage());
		}
	}

	private AdmmReducerContext localMapperOptimization(AdmmMapperContext context) {
		LogisticL2DiffFunction myFunction = new LogisticL2DiffFunction(
				context.getA(), context.getB(), context.getRho(),
				context.getUInitial(), context.getZInitial());
		Ctx optimizationContext = new Ctx(context.getXInitial());

		LOG.info("Minimize Logistic Function using LBFGS....");
		double[] optimum = lbfgs.minimize((DiffFunction) myFunction, 1e-10,
				context.getXInitial());
		for (int d = 0; d < optimum.length; ++d) {
			optimizationContext.m_optimumX[d] = optimum[d];
		}
		double primalObjectiveValue = myFunction
				.evaluatePrimalObjective(optimizationContext.m_optimumX);
		return new AdmmReducerContext(context.getSplitId(),
				context.getUInitial(), optimizationContext.m_optimumX, null,
				primalObjectiveValue, context.getRho(), regularizationFactor, 1);
	}

	private AdmmMapperContext assembleMapperContextFromCache(
			Vector[] inputSplitData, String splitId) throws IOException {
		try {
			AdmmMapperContext preContext = readPreviousAdmmMapperContext(
					splitId, previousIntermediateOutputLocationPath, fs, conf);
			return new AdmmMapperContext(splitId, inputSplitData,
					preContext.getUInitial(), preContext.getXInitial(),
					preContext.getZInitial(), preContext.getRho(),
					preContext.getLambdaValue(),
					preContext.getPrimalObjectiveValue(),
					preContext.getRNorm(), preContext.getSNorm());
		} catch (IOException e) {
			LOG.info("Key not found. Split ID: " + splitId + e.getMessage());
			throw new IOException("Key not found.  Split ID: " + splitId
					+ e.getMessage());
		}
	}
}