package com.b5m.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static com.b5m.admm.AdmmIterationHelper.admmMapperContextToJson;
import static com.b5m.admm.AdmmIterationHelper.mapToJson;

public class AdmmIterationReducer extends
		Reducer<NullWritable, AdmmReducerContextWritable, NullWritable, Text> {

	private static final double THRESHOLD = 0.0001;
	private static final Logger LOG = Logger
			.getLogger(AdmmIterationReducer.class.getName());
	private Map<String, String> outputMap = new HashMap<String, String>();
	private int iteration;
	private int numberOfMappers;
	private boolean regularizeIntercept;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		iteration = Integer.parseInt(conf.get("iteration.number"));
		regularizeIntercept = conf.getBoolean("regularize.intercept", false);
		// TODO
		numberOfMappers = (int) conf.getLong("admm.iteration.num.map.tasks", 0);
	}

	protected void reduce(NullWritable key,
			Iterable<AdmmReducerContextWritable> values, Context context)
			throws IOException, InterruptedException {

		AdmmReducerContextGroup reducerContext = new AdmmReducerContextGroup(
				values.iterator(), numberOfMappers, LOG, iteration);

		setOutputMapperValues(reducerContext);
		context.write(NullWritable.get(), new Text(mapToJson(outputMap)));

		if (reducerContext.getRNorm() > THRESHOLD
				|| reducerContext.getSNorm() > THRESHOLD) {
			context.getCounter(IterationCounter.ITERATION).increment(1);
		}
	}

	private void setOutputMapperValues(AdmmReducerContextGroup context)
			throws IOException {
		double[] zUpdated = getZUpdated(context);
		double[][] xUpdated = context.getXUpdated();
		String[] splitIds = context.getSplitIds();

		for (int mapperNumber = 0; mapperNumber < context.getNumberOfMappers(); mapperNumber++) {
			double[] uUpdated = getUUpdated(context, mapperNumber, zUpdated);
			String currentSplitId = splitIds[mapperNumber];
			AdmmMapperContext admmMapperContext = new AdmmMapperContext(
					currentSplitId, null, null, uUpdated,
					xUpdated[mapperNumber], zUpdated, context.getRho()
							* context.getRhoMultiplier(), context.getLambda(),
					context.getPrimalObjectiveValue(), context.getRNorm(),
					context.getSNorm());

			outputMap.put(currentSplitId,
					admmMapperContextToJson(admmMapperContext));
			LOG.info("Iteration " + iteration + " Reducer Setting splitID "
					+ currentSplitId);
		}
	}

	private double[] getZUpdated(AdmmReducerContextGroup context) {
		int numMappers = context.getNumberOfMappers();
		int numFeatures = context.getNumberOfFeatures();

		double[] xAverage = context.getXUpdatedAverage();
		double[] uAverage = context.getUInitialAverage();
		double[] zUpdated = new double[numFeatures];
		double zMultiplier = (numMappers * context.getRho())
				/ (2 * context.getLambda() + numMappers * context.getRho());

		for (int i = 0; i < numFeatures; i++) {
			if (i == 0 && !regularizeIntercept) {
				zUpdated[i] = xAverage[i] + uAverage[i];
			} else {
				zUpdated[i] = zMultiplier * (xAverage[i] + uAverage[i]);
			}
		}

		return zUpdated;
	}

	private double[] getUUpdated(AdmmReducerContextGroup context,
			int mapperNumber, double[] zUpdated) {
		int numFeatures = context.getNumberOfFeatures();
		double[] uInitial = context.getUInitial()[mapperNumber];
		double[] xUpdated = context.getXUpdated()[mapperNumber];
		double[] uUpdated = new double[numFeatures];
		double rhoMultiplier = context.getRhoMultiplier();

		for (int i = 0; i < numFeatures; i++) {
			uUpdated[i] = (1 / rhoMultiplier)
					* (uInitial[i] + xUpdated[i] - zUpdated[i]);
		}
		return uUpdated;
	}

	public static enum IterationCounter {
		ITERATION
	}
}
