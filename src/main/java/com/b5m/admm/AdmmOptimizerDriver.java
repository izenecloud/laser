package com.b5m.admm;

import com.google.common.base.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;
import java.util.ArrayList;

public class AdmmOptimizerDriver {
	public static void main(String[] args) throws Exception {
		AdmmOptimizerDriver.run(args);
	}

	private static final int DEFAULT_ADMM_ITERATIONS_MAX = 2;
	private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
	private static final String ITERATION_FOLDER_NAME = "iteration_";
	private static final String ITERATION_FOLDER_NAME_FINAL = ITERATION_FOLDER_NAME
			+ "final";

	private static final String STANDARD_ERROR_FOLDER_NAME = "standard-error";
	private static final String BETAS_FOLDER_NAME = "betas";

	public static int run(String[] args) throws IOException, CmdLineException,
			ClassNotFoundException, InterruptedException {
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
		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "sf1");
		conf.setInt("mapred.task.timeout", 6000000);
		conf.setInt("mapred.job.map.memory.mb", 4096);
		conf.setInt("mapred.job.reduce.memory.mb", 4096);

		FileSystem fs = finalOutputBasePath.getFileSystem(conf);
		HadoopUtil.delete(conf, finalOutputBasePath);

		while (!isFinalIteration) {
			long preStatus = 0;
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
				fs.delete(finalOutput);
				fs.rename(currentHdfsResultsPath, finalOutput);
				Path finalOutputBetas = new Path(finalOutputBasePath,
						BETAS_FOLDER_NAME);
				 AdmmResultWriter writer = new AdmmResultWriterBetas();
				 writer.write(conf, fs, finalOutput, finalOutputBetas);
				//
				// // TODO the below could be triggered only in test.
				// boolean isTest = false;
				// if (isTest) {
				// Configuration stdErrConf = new Configuration(conf);
				// Path standardErrorHdfsPath = new Path(finalOutputBasePath,
				// STANDARD_ERROR_FOLDER_NAME);
				// doStandardErrorCalculation(stdErrConf, finalOutput,
				// standardErrorHdfsPath, signalDataLocation,
				// numFeatures, iterationNumber, columnsToExclude,
				// addIntercept, regularizeIntercept,
				// regularizationFactor);
				// }
			}
			iterationNumber++;
		}

		return 0;
	}

	public static void parseArgs(String[] args,
			AdmmOptimizerDriverArguments admmOptimizerDriverArguments)
			throws CmdLineException {
		ArrayList<String> argsList = new ArrayList<String>();

		for (int i = 0; i < args.length; i++) {
			if (AdmmOptimizerDriverArguments.VALID_ARGUMENTS.contains(args[i])) {
				argsList.add(args[i]);
				if (i + 1 < args.length) {
					if (!AdmmOptimizerDriverArguments.VALID_ARGUMENTS
							.contains(args[i + 1])) {
						argsList.add(args[i + 1]);
					}
				}

			}
		}

		new CmdLineParser(admmOptimizerDriverArguments).parseArgument(argsList
				.toArray(new String[argsList.size()]));
	}

	public static void doStandardErrorCalculation(Configuration baseconf,
			Path currentHdfsPath, Path standardErrorHdfsPath,
			String signalDataLocation, int numFeatures, int iterationNumber,
			String columnsToExclude, boolean addIntercept,
			boolean regularizeIntercept, float regularizationFactor)
			throws IOException, ClassNotFoundException, InterruptedException {
		Path signalDataInputLocation = new Path(signalDataLocation);

		Configuration conf = new Configuration(baseconf);
		// No addIntercept option as it would be added in the intermediate data
		// by the Admm iterations.
		// TODO
		conf.set("mapred.child.java.opts", "-Xmx2g");
		conf.set("previous.intermediate.output.location",
				currentHdfsPath.toString());
		conf.set("columns.to.exclude", columnsToExclude);
		conf.setInt("iteration.number", iterationNumber);
		conf.setBoolean("add.intercept", addIntercept);
		conf.setBoolean("regularize.intercept", regularizeIntercept);
		conf.setFloat("regularization.factor", regularizationFactor);
		conf.setInt("signal.data.num.features", numFeatures);

		Job job = new Job(conf);
		job.setJarByClass(AdmmOptimizerDriver.class);
		job.setJobName("ADMM Standard Errors");

		// job.setMapperClass(AdmmStandardErrorsMapper.class);
		// job.setReducerClass(AdmmStandardErrorsReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(AdmmIterationInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		conf.setInt("mapred.num.map.tasks", 2);
		long heapSize = (long) 1024 * 1024 * 128;
		conf.setLong("mapred.mapper.jvm.heap.size", heapSize);

		FileInputFormat.setInputPaths(job, signalDataInputLocation);
		FileOutputFormat.setOutputPath(job, standardErrorHdfsPath);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}

	}

	public static long doAdmmIteration(Configuration baseConf,
			Path previousHdfsPath, Path currentHdfsPath,
			String signalDataLocation, int numFeatures, int iterationNumber,
			String columnsToExclude, boolean addIntercept,
			boolean regularizeIntercept, float regularizationFactor)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration(baseConf);
		conf.set("previous.intermediate.output.location",
				previousHdfsPath.toString());
		conf.setInt("iteration.number", iterationNumber);
		conf.set("columns.to.exclude", columnsToExclude);
		conf.setBoolean("add.intercept", addIntercept);
		conf.setBoolean("regularize.intercept", regularizeIntercept);
		conf.setFloat("regularization.factor", regularizationFactor);
		conf.setInt("signal.data.num.features", numFeatures);

		conf.set("mapred.admin.map.child.java.opts", "Xmx4096M");
		Job job = new Job(conf);
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
		job.setCombinerClass(AdmmIterationCombiner.class);
		job.setReducerClass(AdmmIterationReducer.class);

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
