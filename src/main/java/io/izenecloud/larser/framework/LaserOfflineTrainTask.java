package io.izenecloud.larser.framework;


import io.izenecloud.admm.AdmmOptimizerDriver;
import io.izenecloud.conf.Configuration;
import io.izenecloud.couchbase.CouchbaseConfig;
import io.izenecloud.larser.feature.LaserMessageConsumer;
import io.izenecloud.larser.offline.precompute.Compute;
import io.izenecloud.larser.offline.topn.LaserOfflineResultWriter;
import io.izenecloud.larser.offline.topn.LaserOfflineTopNDriver;
import io.izenecloud.msgpack.MsgpackClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.izenecloud.HDFSHelper.*;

public class LaserOfflineTrainTask implements Job {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOfflineTrainTask.class);

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		String collection = context.getJobDetail().getKey().getGroup();
		LOG.info("Oline Train Task for {}", collection);

		final Path outputPath = Configuration.getInstance()
				.getLaserOfflineOutput(collection);
		final Integer iterationsMaximum = Configuration.getInstance()
				.getMaxIteration(collection);
		final Float regularizationFactor = Configuration.getInstance()
				.getRegularizationFactor(collection);
		final Boolean addIntercept = Configuration.getInstance().addIntercept(
				collection);
		final org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

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

		try {
			final LaserMessageConsumer consumeTask = (LaserMessageConsumer) context
					.getJobDetail().getJobDataMap()
					.get("com.b5m.laser.message.consumer");

			Path admmOutput = new Path(outputPath, "ADMM");
			Path input = consumeTask.nextOfflinePath();
			AdmmOptimizerDriver.run(input, admmOutput, regularizationFactor,
					addIntercept, null, iterationsMaximum, conf);
			HadoopUtil.delete(conf, input);

			LaserOfflineResultWriter writer = new LaserOfflineResultWriter();
			writer.write(collection, fs, new Path(admmOutput,
					AdmmOptimizerDriver.FINAL_MODEL));
			if (consumeTask.modelType().equalsIgnoreCase("per-user")) {
				LOG.info("calculating offline topn clusters for each user, write results to delivery");
				LaserOfflineTopNDriver.run(collection, Configuration
						.getInstance().getTopNClustering(collection), conf);
			}
			if (consumeTask.modelType().equalsIgnoreCase("per-ad")) {
				LOG.info("calculating offline model's stable part, write results to delivery");
				Compute.run(
						Configuration.getInstance().getLaserOfflineOutput(
								collection), conf);
				LOG.info("write orignal model to delivery");
				writeOrigOfflineModel(Configuration.getInstance()
						.getLaserOfflineOutput(collection), fs, conf, client);
			}
			LOG.info("finish offline model");
			client.writeIgnoreRetValue(new Object[0], "finish_offline_model");

		} catch (Exception e) {
			LOG.info(e.getMessage());
		}
	}

	public void writeOrigOfflineModel(Path model, FileSystem fs,
			org.apache.hadoop.conf.Configuration conf, MsgpackClient client)
			throws Exception {
		Vector alpha = readVector(new Path(model, "alpha"), fs, conf);
		Vector beta = readVector(new Path(model, "beta"), fs, conf);
		Matrix A = readMatrix(new Path(model, "A"), fs, conf);

		Object[] req = new Object[3];
		List<Float> alpha1 = new ArrayList<Float>(alpha.size());

		for (int i = 0; i < alpha.size(); i++) {
			alpha1.add((float) (alpha.get(i)));
		}
		req[0] = alpha1;

		List<Float> beta1 = new ArrayList<Float>(beta.size());
		for (int i = 0; i < beta.size(); i++) {
			beta1.add((float) beta.get(i));
		}
		req[1] = beta1;

		List<Float> conjunction = new ArrayList<Float>(A.numRows()
				* A.numCols());
		for (int row = 0; row < A.numRows(); row++) {
			Vector vec = A.viewRow(row);
			for (int col = 0; col < A.numCols(); col++) {
				conjunction.add((float) vec.get(col));
			}
		}
		req[2] = conjunction;
		client.writeIgnoreRetValue(req, "updateLaserOfflineModel");

	}
}
