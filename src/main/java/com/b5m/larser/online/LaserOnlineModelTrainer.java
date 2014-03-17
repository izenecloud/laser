package com.b5m.larser.online;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.b5m.lr.LrIterationDriver;
import com.google.common.base.Optional;

public class LaserOnlineModelTrainer {
	private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;

	private final LaserOnlineModelTrainerArguments laserOnlineArguments = new LaserOnlineModelTrainerArguments();

	public static void main(String[] args) throws Exception {
		LaserOnlineModelTrainer trainer = new LaserOnlineModelTrainer();
		trainer.run(args);
	}

	public int run(String[] args) throws IOException, CmdLineException,
			ClassNotFoundException, InterruptedException {
		parseArgs(args);
		doModelFitting();
		return 0;
	}

	public int doModelFitting() throws ClassNotFoundException, IOException,
			InterruptedException {
		Path signalDataLocation = new Path(laserOnlineArguments.getSignalPath());

		Path finalOutput = new Path(laserOnlineArguments.getOutputPath());
		int numFeatures = laserOnlineArguments.getNumFeatures();
		float regularizationFactor = Optional.fromNullable(
				laserOnlineArguments.getRegularizationFactor()).or(
				DEFAULT_REGULARIZATION_FACTOR);
		boolean addIntercept = Optional.fromNullable(
				laserOnlineArguments.getAddIntercept()).or(false);
		String columnsToExclude = Optional.fromNullable(
				laserOnlineArguments.getColumnsToExclude()).or("");
		Configuration conf = new Configuration();
		return LrIterationDriver.run(signalDataLocation, finalOutput,
				numFeatures, regularizationFactor, addIntercept,
				columnsToExclude, conf);
	}

	private void parseArgs(String[] args) throws CmdLineException {
		ArrayList<String> argsList = new ArrayList<String>(Arrays.asList(args));

		for (int i = 0; i < args.length; i++) {
			if (i % 2 == 0
					&& !LaserOnlineModelTrainerArguments.VALID_ARGUMENTS
							.contains(args[i])) {
				argsList.remove(args[i]);
				argsList.remove(args[i + 1]);
			}
		}

		new CmdLineParser(laserOnlineArguments).parseArgument(argsList
				.toArray(new String[argsList.size()]));
	}
}
