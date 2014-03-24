package com.b5m.lr;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import edu.stanford.nlp.optimization.QNMinimizer;
import static com.b5m.admm.AdmmIterationHelper.*;
import static com.b5m.lr.LrIterationHelper.*;

public class LrIterationMapper extends
		Mapper<Writable, VectorWritable, Text, LrIterationMapContextWritable> {
	private static final double DEFAULT_REGULARIZATION_FACTOR = 0.000001f;

	private int numFeatures;
	private boolean addIntercept;
	private double regularizationFactor;
	private QNMinimizer lbfgs;
	private List<Vector> inputSplitData;
	private String itemId;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		numFeatures = conf.getInt("lr.iteration.number.of.features", 0);
		addIntercept = conf.getBoolean("lr.iteration.add.intercept", true);
		regularizationFactor = conf.getDouble(
				"lr.iteration.regulariztion.factor",
				DEFAULT_REGULARIZATION_FACTOR);
		lbfgs = new QNMinimizer();

		inputSplitData = new LinkedList<Vector>();
		
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		itemId = createItemIdFromHdfsPath(inputSplit.getPath());

	}

	protected void map(Writable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		Vector v = value.get();
		if (addIntercept)
			v.set(0, 1.0);
		inputSplitData.add(v);

		
		LrIterationMapContext mapContext = new LrIterationMapContext(itemId, inputSplitData);
		mapContext = localMapperOptimization(mapContext);
		context.write(new Text(itemId), new LrIterationMapContextWritable(
				mapContext));
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {

	}

	private LrIterationMapContext localMapperOptimization(
			LrIterationMapContext context) {
		LogisticL2DiffFunction tarFunction = new LogisticL2DiffFunction(
				context.getA(), context.getB(), context.getX(),
				regularizationFactor);

		// TODO functionTolerance
		double[] optimum = lbfgs.minimize(tarFunction, 1e-10, context.getX());
		context.setX(optimum);
		return context;
	}
}
