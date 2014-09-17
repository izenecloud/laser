package io.izenecloud.larser.framework;

import io.izenecloud.conf.Configuration;
import io.izenecloud.couchbase.CouchbaseConfig;
import io.izenecloud.larser.feature.LaserMessageConsumer;
import io.izenecloud.larser.online.LaserOnlineModelTrainer;
import io.izenecloud.msgpack.MsgpackClient;

import org.apache.hadoop.fs.Path;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LaserOnlineTrainTask implements Job {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOnlineTrainTask.class);

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		try {
			String collection = context.getJobDetail().getKey().getGroup();

			LOG.info("Online Train Task for {}", collection);

			final Path output = Configuration.getInstance()
					.getLaserOnlineOutput(collection);
			final Float regularizationFactor = Configuration.getInstance()
					.getRegularizationFactor(collection);
			final Boolean addIntercept = Configuration.getInstance()
					.addIntercept(collection);
			final org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			
			conf.set("mapred.job.queue.name", "sf1");
			conf.set("com.b5m.laser.collection", collection);
			conf.set("com.b5m.laser.msgpack.host", Configuration.getInstance()
					.getMsgpackAddress(collection));
			conf.setInt("com.b5m.laser.msgpack.port", Configuration.getInstance()
					.getMsgpackPort(collection));
			conf.set(CouchbaseConfig.CB_INPUT_CLUSTER, io.izenecloud.conf.Configuration
					.getInstance().getCouchbaseCluster(collection));
			conf.set(CouchbaseConfig.CB_INPUT_BUCKET, io.izenecloud.conf.Configuration
					.getInstance().getCouchbaseBucket(collection));
			conf.set(CouchbaseConfig.CB_INPUT_PASSWORD, io.izenecloud.conf.Configuration
					.getInstance().getCouchbasePassword(collection));
			
			final MsgpackClient client = new MsgpackClient(conf);
			
			final LaserMessageConsumer consumeTask = (LaserMessageConsumer) context
					.getJobDetail().getJobDataMap()
					.get("com.b5m.laser.message.consumer");

			Path signalPath = consumeTask.nextOnlinePath();
			LOG.info(
					"Retraining Laser's Online Model, results is flushed to {}",
					output);
			LaserOnlineModelTrainer.run(collection, signalPath, output,
					regularizationFactor, addIntercept, conf);
			LOG.info("finish update online model");
			client.writeIgnoreRetValue(new Object[0], "finish_online_model");
		} catch (Exception e) {
			LOG.info("LaserOnlineTrainerTask failed, {}", e.getStackTrace());
		}
	}

}
