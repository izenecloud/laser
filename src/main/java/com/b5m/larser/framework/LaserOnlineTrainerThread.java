package com.b5m.larser.framework;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.conf.Configuration;
import com.b5m.larser.feature.offline.OfflineFeatureDriver;
import com.b5m.larser.online.LaserOnlineModelTrainer;
import com.b5m.larser.online.LaserOnlineResultWriter;

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
			this.input = conf.getLaserOnlineInput();
			this.output = conf.getLaserOnlineOutput();
			this.conf = new org.apache.hadoop.conf.Configuration();
			this.regularizationFactor = conf.getRegularizationFactor();
			this.addIntercept = conf.addIntercept();
		}

		@Override
		public void run() {
			try {
				final LaserMetaqThread metaqThread = LaserMetaqThread
						.getInstance();
				long minorVersion = metaqThread.getMinorVersion();
				long majorVersion = metaqThread.getMajorVersion();

				metaqThread.incrMinorVersion();
				LOG.info(
						"Update MetaQ's output path, minor version from {} to {}",
						minorVersion, metaqThread.getMinorVersion());

				try {
					Path signalPath = new Path(input,
							Long.toString(majorVersion) + "-"
									+ Long.toString(minorVersion));
					LOG.info(
							"Retraining Laser's Online Model, results is flushed to {}",
							output);
					LaserOnlineModelTrainer.run(signalPath, output,
							regularizationFactor, addIntercept, conf);

					LaserOnlineResultWriter writer = new LaserOnlineResultWriter();
					writer.write(conf, output.getFileSystem(conf), new Path(
							output, "LR"));
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
