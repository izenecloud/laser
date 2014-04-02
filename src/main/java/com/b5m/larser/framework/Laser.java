package com.b5m.larser.framework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.b5m.conf.Configuration;

public class Laser {

	public static void main(String[] args) throws CmdLineException, IOException {
		Laser framework = new Laser();
		framework.run(args);
	}

	private static final Logger LOG = LoggerFactory.getLogger(Laser.class);
	private final LaserArgument laserArgument = new LaserArgument();

	public void run(String[] args) throws CmdLineException, IOException {
		parseArgs(args);
		Configuration conf = Configuration.getInstance();
		LOG.info("Load configure, {}", laserArgument.getConfigure());
		Path path = new Path(laserArgument.getConfigure());
		FileSystem fs = path
				.getFileSystem(new org.apache.hadoop.conf.Configuration());
		conf.load(path, fs);

		LOG.info("Start LaserMetaqThread.");
		LaserMetaqThread.getInstance().start();

		LOG.info("Start LaserOnlineTrainerThread.");
		LaserOnlineTrainerThread.getInstance().start();

		LOG.info("Start LaserOfflineTrainerThread.");
		LaserOfflineTrainerThread.getInstance().start();

		Signal.handle(new Signal("INT"), new SignalHandler() {
			public void handle(Signal sig) {

				LOG.info("Stop LaserMetaqThread");
				try {
					LaserMetaqThread.getInstance().exit();
				} catch (IOException e) {
					e.printStackTrace();
				}

				LOG.info("Stop LaserOnlineTrainerThread.");
				LaserOnlineTrainerThread.getInstance().exit();

				LOG.info("Stop LaserOfflineTrainerThread.");
				LaserOfflineTrainerThread.getInstance().exit();
				System.exit(0);
			}
		});
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
