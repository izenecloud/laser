package com.b5m.lr;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.math.Matrix;

import edu.stanford.nlp.optimization.QNMinimizer;
import static com.b5m.admm.AdmmIterationHelper.*;
import static com.b5m.lr.LrIterationHelper.*;

public class LrIterationMapper extends
		Mapper<LongWritable, Text, Text, LrIterationMapContextWritable> {
	private static final double DEFAULT_REGULARIZATION_FACTOR = 0.000001f;

	private int numFeatures;
	private Set<Integer> columnsToExclude;
	private boolean addIntercept;
	private double regularizationFactor;
	private QNMinimizer lbfgs;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		numFeatures = conf.getInt("lr.iteration.number.of.features", 0);
		String columnsToExclude = conf.get("lr.iteration.columns.to.exclude");
		this.columnsToExclude = getColumnsToExclude(columnsToExclude);
		addIntercept = conf.getBoolean("lr.iteration.add.intercept", true);
		regularizationFactor = conf.getDouble(
				"lr.iteration.regulariztion.factor",
				DEFAULT_REGULARIZATION_FACTOR);
		lbfgs = new QNMinimizer();
	}

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		String itemId = createItemIdFromHdfsPath(inputSplit.getPath());
		Matrix ab = createMatrixFromDataString(value.toString(), numFeatures,
				columnsToExclude, addIntercept);
		LrIterationMapContext mapContext = new LrIterationMapContext(itemId, ab);
		mapContext = localMapperOptimization(mapContext);
		context.write(new Text(itemId), new LrIterationMapContextWritable(mapContext));
	}

	private LrIterationMapContext localMapperOptimization(
			LrIterationMapContext context) {
		LogisticL2DiffFunction tarFunction = new LogisticL2DiffFunction(
				context.getA(), context.getB(), context.getX(),
				regularizationFactor);

		//TODO functionTolerance
		double[] optimum = lbfgs.minimize(tarFunction, 1e-10,
				context.getX());
		context.setX(optimum);
		return context;
	}
}
