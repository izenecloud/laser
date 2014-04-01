package com.b5m.larser.online;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.b5m.larser.feature.online.OnlineFeatureDriver;
import com.b5m.lr.LrIterationDriver;
import com.google.common.base.Optional;

public class LaserOnlineModelTrainer {
	private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;

	private final LaserOnlineModelTrainerArguments laserOnlineArguments = new LaserOnlineModelTrainerArguments();

	public static void main(String[] args) throws Exception {
		LaserOnlineModelTrainer trainer = new LaserOnlineModelTrainer();
		trainer.run(args);
	}

	public static int run(Path input, Path output, Float regularizationFactor,
			Boolean addIntercept, Configuration conf)
			throws ClassNotFoundException, IOException, InterruptedException {
		Path userGroup = new Path(output, "userGroup");
		OnlineFeatureDriver.run(input, userGroup, conf);

		conf.set("mapred.job.queue.name", "sf1");
		conf.setInt("mapred.task.timeout", 6000000);
		conf.setInt("mapred.job.map.memory.mb", 4096);
		conf.setInt("mapred.job.reduce.memory.mb", 4096);

		Path lrOutput = new Path(output, "LR");
		LrIterationDriver.run(userGroup, lrOutput, regularizationFactor,
				addIntercept, conf);

		HadoopUtil.delete(conf, userGroup);
		return 0;
	}

	public int run(String[] args) throws IOException, CmdLineException,
			ClassNotFoundException, InterruptedException {
		parseArgs(args);

		Path input = new Path(laserOnlineArguments.getSignalPath());
		Path output = new Path(laserOnlineArguments.getOutputPath());
		Float regularizationFactor = Optional.fromNullable(
				laserOnlineArguments.getRegularizationFactor()).or(
				DEFAULT_REGULARIZATION_FACTOR);
		Boolean addIntercept = Optional.fromNullable(
				laserOnlineArguments.getAddIntercept()).or(false);
		Configuration conf = new Configuration();

		Path userGroup = new Path(output, "userGroup");
		OnlineFeatureDriver.run(input, userGroup, conf);

		conf.set("mapred.job.queue.name", "sf1");
		conf.setInt("mapred.task.timeout", 6000000);
		conf.setInt("mapred.job.map.memory.mb", 4096);
		conf.setInt("mapred.job.reduce.memory.mb", 4096);

		Path lrOutput = new Path(output, "LR");
		LrIterationDriver.run(userGroup, lrOutput, regularizationFactor,
				addIntercept, conf);

		HadoopUtil.delete(conf, userGroup);
		return 0;
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
