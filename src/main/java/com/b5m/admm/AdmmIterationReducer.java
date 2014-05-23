package com.b5m.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.b5m.admm.AdmmIterationHelper.*;

public class AdmmIterationReducer
		extends
		Reducer<NullWritable, AdmmReducerContextWritable, NullWritable, AdmmReducerContextWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(AdmmIterationReducer.class);

	private static final double SQUARE_ROOT_POWER = 0.5;
	private static final double RHO_INCREMENT_MULTIPLIER = 1.5;
	private static final double RHO_DECREMENT_MULTIPLIER = 1.5;
	private static final double RHO_UPDATE_THRESHOLD = 5;
	private static final double THRESHOLD = 0.001;

	private boolean regularizeIntercept;
	private double[] xUpdated;
	private double[] uInital;
	private long count;
	private double rho;
	private double lambda;
	private double zMultiplier;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		regularizeIntercept = conf.getBoolean("regularize.intercept", false);

		xUpdated = null;
		uInital = null;
		count = 0;
	}

	protected void reduce(NullWritable key,
			Iterable<AdmmReducerContextWritable> values, Context context)
			throws IOException, InterruptedException {

		for (AdmmReducerContextWritable reducerContextWritable : values) {
			AdmmReducerContext reducerContext = reducerContextWritable.get();
			if (null == this.xUpdated) {
				this.xUpdated = reducerContext.getXUpdated();
				this.uInital = reducerContext.getUInitial();
				this.rho = reducerContext.getRho();
				this.lambda = reducerContext.getLambdaValue();
				this.count = reducerContext.getCount();
			} else {
				double[] xUpdated = reducerContext.getXUpdated();
				for (int i = 0; i < xUpdated.length; i++) {
					this.xUpdated[i] += xUpdated[i];
				}
				double[] uInital = reducerContext.getUInitial();
				for (int i = 0; i < uInital.length; i++) {
					this.uInital[i] += uInital[i];
				}
				this.count += reducerContext.getCount();
			}
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		for (int i = 0; i < xUpdated.length; i++) {
			xUpdated[i] /= count;
		}

		for (int i = 0; i < uInital.length; i++) {
			uInital[i] /= count;
		}

		this.zMultiplier = this.rho * this.count
				/ (this.rho * this.count + 2 * this.lambda);
		double[] zUpdated = new double[xUpdated.length];
		for (int i = 0; i < zUpdated.length; i++) {
			zUpdated[i] /= this.count;
			if (i == 0 && !regularizeIntercept) {
				zUpdated[i] = xUpdated[i] + uInital[i];
			} else {
				zUpdated[i] = (xUpdated[i] + uInital[i]) * this.zMultiplier;
			}
		}

		Configuration conf = context.getConfiguration();
		Path outputPath = FileOutputFormat.getOutputPath(context);
		FileSystem fs = outputPath.getFileSystem(conf);

		LOG.info("calculating sNorm and rNorm");
		double sNorm = calculateSNorm(uInital, xUpdated);
		double rNorm = calculateRNorm(outputPath, xUpdated, fs, conf);

		LOG.info("rNorm = {}, sNorm = {}", rNorm, sNorm);
		if (rNorm > THRESHOLD || sNorm > THRESHOLD) {
			context.getCounter(IterationCounter.ITERATION).increment(1);
			LOG.info("increment IterationCounter = {}",
					context.getCounter(IterationCounter.ITERATION).getValue());
		}

		double rhoMultiplier = 0;
		if (rNorm > RHO_UPDATE_THRESHOLD * sNorm) {
			rhoMultiplier = RHO_INCREMENT_MULTIPLIER;
		} else if (sNorm > RHO_UPDATE_THRESHOLD * rNorm) {
			rhoMultiplier = 1.0 / RHO_DECREMENT_MULTIPLIER;
		} else {
			rhoMultiplier = 1.0;
		}

		AdmmReducerContext reducerContext = new AdmmReducerContext(null, null,
				null, zUpdated, 0.0, rho * rhoMultiplier, lambda, (long) 0);

		context.write(NullWritable.get(), new AdmmReducerContextWritable(
				reducerContext));
	}

	public static enum IterationCounter {
		ITERATION
	}

	private double calculateRNorm(Path outputPath, double[] xUpdated,
			FileSystem fs, Configuration conf) {
		double result = 0.0;
		try {
			result = calculateR(outputPath, fs, conf, xUpdated);
		} catch (IOException e) {
			e.printStackTrace();
		}
		result = Math.pow(result, SQUARE_ROOT_POWER);
		return result;
	}

	private double calculateSNorm(double[] xInitial, double[] xUpdated) {
		double result = 0.0;
		for (int i = 0; i < xUpdated.length; i++) {
			result += Math.pow(xUpdated[i] - xInitial[i], 2);
		}
		result *= Math.pow(rho, 2);
		result *= this.count;
		result = Math.pow(result, SQUARE_ROOT_POWER);

		return result;
	}

}
