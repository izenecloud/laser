package com.b5m.larser.offline.topn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.conf.Configuration;
import com.b5m.larser.framework.LaserArgument;

public class LaserOfflineTopNT {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOfflineTopNT.class);
	private static final LaserArgument laserArgument = new LaserArgument();

	public static void main(String[] args) throws CmdLineException,
			IOException, ClassNotFoundException, InterruptedException {

		parseArgs(args);
		Configuration conf = Configuration.getInstance();
		LOG.info("Load configure, {}", laserArgument.getConfigure());
		Path path = new Path(laserArgument.getConfigure());
		FileSystem fs = FileSystem
				.get(new org.apache.hadoop.conf.Configuration());
		// path
		// .getFileSystem(new org.apache.hadoop.conf.Configuration());
		conf.load(path, fs);

		LOG.info("calculating offline topn clusters for each user, write results to msgpack");
		LaserOfflineTopNDriver.run(Configuration.getInstance()
				.getTopNClustering(),
				new org.apache.hadoop.conf.Configuration());

	}

	private static void parseArgs(String[] args) throws CmdLineException {
		ArrayList<String> argsList = new ArrayList<String>(Arrays.asList(args));

		for (int i = 0; i < args.length; i++) {
			if (i % 2 == 0 && !LaserArgument.VALID_ARGUMENTS.contains(args[i])) {
				argsList.remove(args[i]);
				argsList.remove(args[i + 1]);
			}
		}

		new CmdLineParser(laserArgument).parseArgument(argsList
				.toArray(new String[argsList.size()]));
	}
}
