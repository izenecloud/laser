package com.b5m.tool;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.kohsuke.args4j.CmdLineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.admm.AdmmOptimizerDriver;
import com.b5m.conf.Configuration;
import com.b5m.larser.feature.offline.OfflineFeatureDriver;
import com.b5m.larser.framework.LaserArgument;
import com.b5m.larser.offline.topn.LaserOfflineResultWriter;

public class LaserOfflineTrainerT {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOfflineTrainerT.class);

	public static void main(String[] args) throws CmdLineException,
			IOException, ClassNotFoundException, InterruptedException {
		LaserArgument.parseArgs(args);

		Path input = Configuration.getInstance().getMetaqOutput();
		Path output = Configuration.getInstance().getLaserOfflineOutput();

		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		conf.set("mapred.job.queue.name", "sf1");

		LOG.info("Retraining Laser's Offline Model, result = {}", output);
		Path signalData = new Path(output, "ADMM_SIGNAL");
		OfflineFeatureDriver.run(new Path(input, "*-*"), signalData, conf);

		Path admmOutput = new Path(output, "ADMM");
		AdmmOptimizerDriver.run(signalData, admmOutput, Configuration
				.getInstance().getRegularizationFactor(), Configuration
				.getInstance().addIntercept(), null, Configuration
				.getInstance().getMaxIteration(), conf);
		HadoopUtil.delete(conf, signalData);

		LaserOfflineResultWriter writer = new LaserOfflineResultWriter();
		writer.write(conf, output.getFileSystem(conf), new Path(admmOutput,
				AdmmOptimizerDriver.FINAL_MODEL), output);

	}

}
