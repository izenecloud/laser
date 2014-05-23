package com.b5m.larser.framework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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
		LaserArgument.parseArgs(args);
		Laser framework = new Laser();
		framework.run();
	}

	private static final Logger LOG = LoggerFactory.getLogger(Laser.class);

	public void run() throws CmdLineException, IOException {

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
}
