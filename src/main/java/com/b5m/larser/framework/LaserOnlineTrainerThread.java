package com.b5m.larser.framework;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.conf.Configuration;
import com.b5m.larser.online.LaserOnlineModelTrainer;

public class LaserOnlineTrainerThread {
	private static LaserOnlineTrainerThread thread = null;
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOnlineTrainerThread.class);

	public static synchronized LaserOnlineTrainerThread getInstance() {
		if (null == thread) {
			thread = new LaserOnlineTrainerThread();
		}
		return thread;
	}

	private final Timer timer;

	private LaserOnlineTrainerThread() {
		timer = new Timer();

	}

	public void start() {
		Configuration conf = Configuration.getInstance();
		Long freq = conf.getLaserOnlineRetrainingFreqency() * 1000;
		timer.scheduleAtFixedRate(new LaserOnlineTrainerTask(), freq, freq);
	}

	public void exit() {
		timer.cancel();
	}

	class LaserOnlineTrainerTask extends TimerTask {

		private final Path input;
		private final Path output;
		private org.apache.hadoop.conf.Configuration conf;
		private final Float regularizationFactor;
		private final Boolean addIntercept;

		public LaserOnlineTrainerTask() {
			Configuration conf = Configuration.getInstance();
			this.input = conf.getMetaqOutput();
			this.output = conf.getLaserOnlineOutput();
			this.conf = new org.apache.hadoop.conf.Configuration();
			this.regularizationFactor = conf.getRegularizationFactor();
			this.addIntercept = conf.addIntercept();
		}

		@Override
		public void run() {
			try {
				Path offlineModelPath = Configuration.getInstance()
						.getLaserOfflineOutput();
				FileSystem fs = offlineModelPath.getFileSystem(conf);

				if (!fs.exists(offlineModelPath)) {
					LOG.info("Laser offline model does not exit");
					return;
				}

				Path offlineOutput = com.b5m.conf.Configuration.getInstance()
						.getLaserOfflineOutput();
				Path delta = new Path(offlineOutput, "delta");
				Path beta = new Path(offlineOutput, "beta");
				Path A = new Path(offlineOutput, "A");
				if (!fs.exists(A) || !fs.exists(beta) || fs.exists(delta)) {

					LOG.info("Laser offline model does not exit");
					return;
				}

				final LaserMetaqThread metaqThread = LaserMetaqThread
						.getInstance();
				long minorVersion = metaqThread.getMinorVersion();
				long majorVersion = metaqThread.getMajorVersion();

				metaqThread.incrMinorVersion();
				LOG.info(
						"Update MetaQ's output path, minor version from {} to {}",
						minorVersion, metaqThread.getMinorVersion());

				Path signalPath = new Path(input, Long.toString(majorVersion)
						+ "-" + Long.toString(minorVersion));
				LOG.info(
						"Retraining Laser's Online Model, results is flushed to {}",
						output);
				LaserOnlineModelTrainer.run(signalPath, output,
						regularizationFactor, addIntercept, conf);
			} catch (Exception e) {
				LOG.info("LaserOnlineTrainerTask failed, {}", e.getStackTrace());
			}
		}
	}
}
