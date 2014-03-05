package com.b5m.admm;

import com.google.common.base.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

public class AdmmOptimizerDriver extends Configured implements Tool {

	private static final int DEFAULT_ADMM_ITERATIONS_MAX = 2;
	private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
	private static final String ITERATION_FOLDER_NAME = "iteration_";
	private static final String ITERATION_FOLDER_NAME_FINAL = ITERATION_FOLDER_NAME
			+ "final";

	private static final String STANDARD_ERROR_FOLDER_NAME = "standard-error";
	private static final String BETAS_FOLDER_NAME = "betas";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new AdmmOptimizerDriver(), args);
	}

	public int run(String[] args) throws IOException, CmdLineException {
		AdmmOptimizerDriverArguments admmOptimizerDriverArguments = new AdmmOptimizerDriverArguments();
		parseArgs(args, admmOptimizerDriverArguments);

		String signalDataLocation = admmOptimizerDriverArguments
				.getSignalPath();

		Path finalOutputBasePath = new Path(
				admmOptimizerDriverArguments.getOutputPath());
		String intermediateHdfsBaseString = finalOutputBasePath.toString()
				+ "/tmp";
		int numFeatures = admmOptimizerDriverArguments.getNumFeatures();
		int iterationsMaximum = Optional.fromNullable(
				admmOptimizerDriverArguments.getIterationsMaximum()).or(
				DEFAULT_ADMM_ITERATIONS_MAX);
		float regularizationFactor = Optional.fromNullable(
				admmOptimizerDriverArguments.getRegularizationFactor()).or(
				DEFAULT_REGULARIZATION_FACTOR);
		boolean addIntercept = Optional.fromNullable(
				admmOptimizerDriverArguments.getAddIntercept()).or(false);
		boolean regularizeIntercept = Optional.fromNullable(
				admmOptimizerDriverArguments.getRegularizeIntercept())
				.or(false);
		String columnsToExclude = Optional.fromNullable(
				admmOptimizerDriverArguments.getColumnsToExclude()).or("");

		int iterationNumber = 0;
		boolean isFinalIteration = false;
		FileSystem fs = finalOutputBasePath.getFileSystem(getConf());

		while (!isFinalIteration) {
			long preStatus = 0;
			JobConf conf = new JobConf(getConf(), AdmmOptimizerDriver.class);
			Path previousHdfsResultsPath = new Path(intermediateHdfsBaseString
					+ ITERATION_FOLDER_NAME + (iterationNumber - 1));
			Path currentHdfsResultsPath = new Path(intermediateHdfsBaseString
					+ ITERATION_FOLDER_NAME + iterationNumber);

			long curStatus = doAdmmIteration(conf, previousHdfsResultsPath,
					currentHdfsResultsPath, signalDataLocation, numFeatures,
					iterationNumber, columnsToExclude, addIntercept,
					regularizeIntercept, regularizationFactor);
			isFinalIteration = convergedOrMaxed(curStatus, preStatus,
					iterationNumber, iterationsMaximum);

			if (isFinalIteration) {
				Path finalOutput = new Path(finalOutputBasePath,
						ITERATION_FOLDER_NAME_FINAL);
				fs.rename(currentHdfsResultsPath, finalOutput);
				Path finalOutputBetas = new Path(finalOutputBasePath,
						BETAS_FOLDER_NAME);
				AdmmResultWriter writer = new AdmmResultWriterBetas();
				writer.write(conf, fs, finalOutput, finalOutputBetas);

				JobConf stdErrConf = new JobConf(getConf(),
						AdmmOptimizerDriver.class);
				Path standardErrorHdfsPath = new Path(finalOutputBasePath,
						STANDARD_ERROR_FOLDER_NAME);
				// TODO the below could be triggered only in test.
				boolean isTest = false;
				if (isTest) {
					doStandardErrorCalculation(stdErrConf, finalOutput,
							standardErrorHdfsPath, signalDataLocation,
							numFeatures, iterationNumber, columnsToExclude,
							addIntercept, regularizeIntercept,
							regularizationFactor);
				}
			}
			iterationNumber++;
		}

		return 0;
	}

	private void parseArgs(String[] args,
			AdmmOptimizerDriverArguments admmOptimizerDriverArguments)
			throws CmdLineException {
		ArrayList<String> argsList = new ArrayList<String>(Arrays.asList(args));

		for (int i = 0; i < args.length; i++) {
			if (i % 2 == 0
					&& !AdmmOptimizerDriverArguments.VALID_ARGUMENTS
							.contains(args[i])) {
				argsList.remove(args[i]);
				argsList.remove(args[i + 1]);
			}
		}

		new CmdLineParser(admmOptimizerDriverArguments).parseArgument(argsList
				.toArray(new String[argsList.size()]));
	}

	public void doStandardErrorCalculation(JobConf conf, Path currentHdfsPath,
			Path standardErrorHdfsPath, String signalDataLocation,
			int numFeatures, int iterationNumber, String columnsToExclude,
			boolean addIntercept, boolean regularizeIntercept,
			float regularizationFactor) throws IOException {
		Path signalDataInputLocation = new Path(signalDataLocation);

		// No addIntercept option as it would be added in the intermediate data
		// by the Admm iterations.
		conf.setJobName("ADMM Standard Errors");
		conf.set("mapred.child.java.opts", "-Xmx2g");
		conf.set("previous.intermediate.output.location",
				currentHdfsPath.toString());
		conf.set("columns.to.exclude", columnsToExclude);
		conf.setInt("iteration.number", iterationNumber);
		conf.setBoolean("add.intercept", addIntercept);
		conf.setBoolean("regularize.intercept", regularizeIntercept);
		conf.setFloat("regularization.factor", regularizationFactor);
		conf.setInt("signal.data.num.features", numFeatures);

		conf.setMapperClass(AdmmStandardErrorsMapper.class);
		conf.setReducerClass(AdmmStandardErrorsReducer.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(SignalInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, signalDataInputLocation);
		FileOutputFormat.setOutputPath(conf, standardErrorHdfsPath);

		JobClient.runJob(conf);
	}

	public long doAdmmIteration(JobConf conf, Path previousHdfsPath,
			Path currentHdfsPath, String signalDataLocation, int numFeatures,
			int iterationNumber, String columnsToExclude, boolean addIntercept,
			boolean regularizeIntercept, float regularizationFactor)
			throws IOException {
		Path signalDataInputLocation = new Path(signalDataLocation);

		conf.setJobName("ADMM Optimizer " + iterationNumber);
		conf.set("mapred.child.java.opts", "-Xmx2g");
		conf.set("previous.intermediate.output.location",
				previousHdfsPath.toString());
		conf.setInt("iteration.number", iterationNumber);
		conf.set("columns.to.exclude", columnsToExclude);
		conf.setBoolean("add.intercept", addIntercept);
		conf.setBoolean("regularize.intercept", regularizeIntercept);
		conf.setFloat("regularization.factor", regularizationFactor);
		conf.setInt("signal.data.num.features", numFeatures);

		conf.setMapperClass(AdmmIterationMapper.class);
		conf.setReducerClass(AdmmIterationReducer.class);
		conf.setMapOutputKeyClass(NullWritable.class);
		conf.setMapOutputValueClass(AdmmReducerContextWritable.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setInt("mapred.num.map.tasks", 2);
		long heapSize = (long) 1024 * 1024 * 128;
		conf.setLong("mapred.mapper.jvm.heap.size", heapSize);

		conf.setInputFormat(SignalInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, signalDataInputLocation);
		FileSystem fs = signalDataInputLocation.getFileSystem(conf);
		if (fs.exists(currentHdfsPath)) {
			fs.delete(currentHdfsPath, true);
		}
		FileOutputFormat.setOutputPath(conf, currentHdfsPath);

		RunningJob job = JobClient.runJob(conf);

		return job.getCounters()
				.findCounter(AdmmIterationReducer.IterationCounter.ITERATION)
				.getValue();
	}

	private boolean convergedOrMaxed(long curStatus, long preStatus,
			int iterationNumber, int iterationsMaximum) {
		return curStatus <= preStatus || iterationNumber >= iterationsMaximum;
	}
}
