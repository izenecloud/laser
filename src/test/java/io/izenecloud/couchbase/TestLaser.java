package io.izenecloud.couchbase;

import io.izenecloud.conf.Configuration;
import io.izenecloud.couchbase.CouchbaseInputFormatIT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLaser {

	public static void main(String[] args) throws CmdLineException, IOException {
		TestLaser framework = new TestLaser();
		framework.run(args);
	}

	private static final Logger LOG = LoggerFactory.getLogger(TestLaser.class);
	private final LaserArgument laserArgument = new LaserArgument();

	public void run(String[] args) throws CmdLineException, IOException {
		parseArgs(args);
		Configuration conf = Configuration.getInstance();
		LOG.info("Load configure, {}", laserArgument.getConfigure());
		Path path = new Path(laserArgument.getConfigure());
		FileSystem fs = FileSystem
				.get(new org.apache.hadoop.conf.Configuration());
		conf.load(path, fs);

		try {
			new CouchbaseInputFormatIT().test();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void parseArgs(String[] args) throws CmdLineException {
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
