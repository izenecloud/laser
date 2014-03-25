package com.b5m.lr;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.nlp.optimization.QNMinimizer;
import static com.b5m.admm.AdmmIterationHelper.*;
import static com.b5m.lr.LrIterationHelper.*;

public class LrIterationMapper
		extends
		Mapper<IntWritable, ListWritable, IntWritable, LrIterationMapContextWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(LrIterationMapper.class);
	private static final double DEFAULT_REGULARIZATION_FACTOR = 0.000001f;

	private boolean addIntercept;
	private double regularizationFactor;
	private QNMinimizer lbfgs;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		addIntercept = conf.getBoolean("lr.iteration.add.intercept", true);
		regularizationFactor = conf.getDouble(
				"lr.iteration.regulariztion.factor",
				DEFAULT_REGULARIZATION_FACTOR);
		lbfgs = new QNMinimizer();
		lbfgs.setRobustOptions();
		// lbfgs.useOWLQN(true, DEFAULT_REGULARIZATION_FACTOR);
	}

	@SuppressWarnings("unchecked")
	protected void map(IntWritable key, ListWritable valueWritable,
			Context context) throws IOException, InterruptedException {
		int itemId = key.get();
		// long sTime = System.nanoTime();
		List<Vector> inputSplitData = null;
		try {
			inputSplitData = valueWritable.getListClass().newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

		List<Writable> value = valueWritable.get();
		Iterator<Writable> iterator = value.iterator();
		while (iterator.hasNext()) {
			Vector v = ((VectorWritable) (iterator.next())).get();
			if (addIntercept)
				v.set(0, 1.0);
			inputSplitData.add(v);
		}
		// long eTime = System.nanoTime();
		// LOG.info("Time for construct A and b, {}", eTime - sTime);
		LrIterationMapContext mapContext = new LrIterationMapContext(itemId,
				inputSplitData);
		mapContext = localMapperOptimization(mapContext);
		context.write(key, new LrIterationMapContextWritable(mapContext));
	}

	private LrIterationMapContext localMapperOptimization(
			LrIterationMapContext context) {
		LogisticL2DiffFunction logistic = new LogisticL2DiffFunction(
				context.getA(), context.getB(), context.getX(),
				regularizationFactor);
		double[] optimum = lbfgs.minimize(logistic, 1e-6, context.getX());
		context.setX(optimum);
		return context;
	}
}
