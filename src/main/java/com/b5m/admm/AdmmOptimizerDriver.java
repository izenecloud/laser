package com.b5m.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.mortbay.log.Log;

import java.io.IOException;

public class AdmmOptimizerDriver {
	private static final int DEFAULT_ADMM_ITERATIONS_MAX = 2;
	private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
	private static final String ITERATION_FOLDER_NAME = "iteration_";
	public static final String FINAL_MODEL = "FINAL_MODEL";

	public static int run(Path signalData, Path output,
			Float regularizationFactor, Boolean addIntercept,
			Boolean regularizeIntercept, Integer iterationsMaximum,
			Configuration baseConf) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration(baseConf);

		float thisRegularizationFactor = null == regularizationFactor ? DEFAULT_REGULARIZATION_FACTOR
				: regularizationFactor;
		boolean thisAddIntercept = null == addIntercept ? true : addIntercept;
		boolean thisRegularizeIntercept = null == regularizeIntercept ? false
				: regularizeIntercept;
		int thisIterationsMaximum = null == iterationsMaximum ? DEFAULT_ADMM_ITERATIONS_MAX
				: iterationsMaximum;

		int iterationNumber = 0;
		boolean isFinalIteration = false;
		conf.set("mapred.job.queue.name", "sf1");
		conf.setInt("mapred.task.timeout", 6000000);
		conf.setInt("mapred.job.map.memory.mb", 4096);
		conf.setInt("mapred.job.reduce.memory.mb", 4096);

		FileSystem fs = output.getFileSystem(conf);
		HadoopUtil.delete(conf, output);

		String intermediateHdfsBaseString = output.toString() + "/Iteration/";

		while (!isFinalIteration) {
			long preStatus = 0;
			Path previousHdfsResultsPath = new Path(intermediateHdfsBaseString
					+ ITERATION_FOLDER_NAME + (iterationNumber - 1));
			Path currentHdfsResultsPath = new Path(intermediateHdfsBaseString
					+ ITERATION_FOLDER_NAME + iterationNumber);

			long curStatus = doAdmmIteration(conf, previousHdfsResultsPath,
					currentHdfsResultsPath, signalData, iterationNumber,
					thisAddIntercept, thisRegularizeIntercept,
					thisRegularizationFactor);
			Log.info("curStatus = {}", curStatus);
			isFinalIteration = convergedOrMaxed(curStatus, preStatus,
					iterationNumber, thisIterationsMaximum);

			if (isFinalIteration) {
				Path finalOutput = new Path(output, FINAL_MODEL);
				fs.delete(finalOutput, true);
				fs.rename(new Path(currentHdfsResultsPath, "Z"), finalOutput);
			}
			iterationNumber++;
		}

		return 0;
	}

	public static long doAdmmIteration(Configuration baseConf,
			Path previousHdfsPath, Path currentHdfsPath,
			Path signalDataLocation, int iterationNumber, boolean addIntercept,
			boolean regularizeIntercept, float regularizationFactor)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration(baseConf);
		conf.set("previous.intermediate.output.location",
				previousHdfsPath.toString());
		conf.setInt("iteration.number", iterationNumber);
		conf.setBoolean("add.intercept", addIntercept);
		conf.setBoolean("regularize.intercept", regularizeIntercept);
		conf.setFloat("regularization.factor", regularizationFactor);

		Job job = Job.getInstance(conf);
		job.setJarByClass(AdmmOptimizerDriver.class);
		job.setJobName("ADMM Optimizer " + iterationNumber);
		AdmmIterationInputFormat.setNumMapTasks(job, 240);

		FileInputFormat.setInputPaths(job, signalDataLocation);
		FileOutputFormat.setOutputPath(job, currentHdfsPath);

		job.setInputFormatClass(AdmmIterationInputFormat.class);
		job.setOutputFormatClass(AdmmIterationOutputFormat.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(AdmmReducerContextWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(AdmmReducerContextWritable.class);

		job.setMapperClass(AdmmIterationMapper.class);
		// job.setCombinerClass(AdmmIterationCombiner.class);
		job.setReducerClass(AdmmIterationReducer.class);
		// job.setNumReduceTasks(12);

		HadoopUtil.delete(conf, currentHdfsPath);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}

		return job.getCounters()
				.findCounter(AdmmIterationReducer.IterationCounter.ITERATION)
				.getValue();
	}

	private static boolean convergedOrMaxed(long curStatus, long preStatus,
			int iterationNumber, int iterationsMaximum) {
		return curStatus <= preStatus || iterationNumber >= iterationsMaximum;
	}
}
