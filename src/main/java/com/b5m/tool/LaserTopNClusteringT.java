package com.b5m.tool;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.conf.Configuration;
import com.b5m.larser.framework.LaserArgument;
import com.b5m.larser.offline.topn.LaserOfflineTopNDriver;

public class LaserTopNClusteringT {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserTopNClusteringT.class);

	public static void main(String[] args) throws CmdLineException,
			IOException, ClassNotFoundException, InterruptedException {

		LaserArgument.parseArgs(args);

		LOG.info("calculating offline topn clusters for each user, write results to msgpack");
		LaserOfflineTopNDriver.run(Configuration.getInstance()
				.getTopNClustering(),
				new org.apache.hadoop.conf.Configuration());

	}

}
