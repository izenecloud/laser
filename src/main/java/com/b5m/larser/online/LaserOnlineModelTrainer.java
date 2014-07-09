package com.b5m.larser.online;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.larser.feature.online.OnlineFeatureDriver;
import com.b5m.lr.LrIterationDriver;

public class LaserOnlineModelTrainer {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOnlineModelTrainer.class);

	public static int run(String collection, Path input, Path output,
			Float regularizationFactor, Boolean addIntercept, Configuration conf)
			throws ClassNotFoundException, IOException, InterruptedException {
		Path groupByIdendifier = new Path(output, "groupByIdendifier");
		try {
			if (0 == OnlineFeatureDriver.run(collection, input, groupByIdendifier, conf)) {
				return 0;
			}
		} catch (IllegalStateException e) {
			LOG.error("the online feature generate phase failed, "
					+ e.getMessage());
			throw e;
		}
		conf.setInt("mapred.task.timeout", 6000000);

		Path lrOutput = new Path(output, "LR");
		LrIterationDriver.run(collection, groupByIdendifier, lrOutput,
				regularizationFactor, addIntercept, conf);
		HadoopUtil.delete(conf, groupByIdendifier);
		return 0;
	}
}
