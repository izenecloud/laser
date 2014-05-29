package com.b5m.larser.framework;

import org.apache.hadoop.fs.Path;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.conf.Configuration;
import com.b5m.larser.feature.LaserMessageConsumer;
import com.b5m.larser.online.LaserOnlineModelTrainer;

public class LaserOnlineTrainTask implements Job {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOnlineTrainTask.class);

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		try {
			String collection = context.getJobDetail().getGroup();

			LOG.info("Online Train Task for {}", collection);

			final Path input = Configuration.getInstance().getMetaqOutput(
					collection);
			final Path output = Configuration.getInstance()
					.getLaserOnlineOutput(collection);
			final Float regularizationFactor = Configuration.getInstance()
					.getRegularizationFactor(collection);
			final Boolean addIntercept = Configuration.getInstance()
					.addIntercept(collection);
			final org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			conf.set("mapred.job.queue.name", "sf1");
			conf.set("com.b5m.msgpack.collection", collection);

			final LaserMessageConsumer consumeTask = LaserMessageConsumeTask
					.getInstance().getLaserMessageConsumer(collection);
			long minorVersion = consumeTask.getMinorVersion();
			long majorVersion = consumeTask.getMajorVersion();

			consumeTask.incrMinorVersion();
			LOG.info("Update MetaQ's output path, minor version from {} to {}",
					minorVersion, consumeTask.getMinorVersion());
			Path signalPath = new Path(input, Long.toString(majorVersion) + "-"
					+ Long.toString(minorVersion));
			LOG.info(
					"Retraining Laser's Online Model, results is flushed to {}",
					output);
			LaserOnlineModelTrainer.run(collection, signalPath, output,
					regularizationFactor, addIntercept, conf);
		} catch (Exception e) {
			LOG.info("LaserOnlineTrainerTask failed, {}", e.getStackTrace());
		}
	}

}
