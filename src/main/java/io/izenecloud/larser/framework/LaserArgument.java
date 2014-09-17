package io.izenecloud.larser.framework;

import io.izenecloud.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LaserArgument {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserArgument.class);
	public static final Set<String> VALID_ARGUMENTS = new HashSet<String>(
			Arrays.asList("-configure"));

	@Option(name = "-configure", required = true, handler = StringOptionHandler.class)
	private String configure;

	public String getConfigure() {
		return configure;
	}

	public static void parseArgs(String[] args) throws CmdLineException,
			IOException {
		ArrayList<String> argsList = new ArrayList<String>(Arrays.asList(args));

		for (int i = 0; i < args.length; i++) {
			if (i % 2 == 0 && !LaserArgument.VALID_ARGUMENTS.contains(args[i])) {
				argsList.remove(args[i]);
				argsList.remove(args[i + 1]);
			}
		}

		final LaserArgument laserArgument = new LaserArgument();
		new CmdLineParser(laserArgument).parseArgument(argsList
				.toArray(new String[argsList.size()]));

		Configuration conf = Configuration.getInstance();
		LOG.info("Load configure, {}", laserArgument.getConfigure());
		Path path = new Path(laserArgument.getConfigure());
		FileSystem fs = FileSystem
				.get(new org.apache.hadoop.conf.Configuration());
		conf.load(path, fs);
	}
}
