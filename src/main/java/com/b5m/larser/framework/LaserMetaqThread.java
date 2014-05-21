package com.b5m.larser.framework;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.conf.Configuration;
import com.b5m.larser.feature.LaserFeatureListenser;
import com.b5m.larser.feature.UserProfileHelper;
import com.b5m.metaq.Consumer;
import com.taobao.metamorphosis.exception.MetaClientException;

public class LaserMetaqThread {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserMetaqThread.class);
	private static LaserMetaqThread metaqThread = null;

	public static synchronized LaserMetaqThread getInstance()
			throws IOException {
		if (null == metaqThread) {
			metaqThread = new LaserMetaqThread();
		}
		return metaqThread;
	}

	class LaserMetaqTask extends Thread {
		private final Consumer consumer;

		public LaserMetaqTask() throws MetaClientException {
			consumer = Consumer.getInstance();
		}

		public void run() {
			try {
				LOG.info("prepare to subscrible metaq, listener = {}", listener);
				consumer.subscribe(listener);
			} catch (MetaClientException e) {
				e.printStackTrace();
			}
		}

		public void exit() throws IOException {
			consumer.shutdown();

			Path serializePath = Configuration.getInstance()
					.getUserFeatureSerializePath();
			FileSystem fs = serializePath
					.getFileSystem(new org.apache.hadoop.conf.Configuration());
			DataOutputStream out = fs.create(serializePath);

			UserProfileHelper.getInstance().write(out);
			out.close();
			// LOG.info(UserProfileHelper.getInstance().toString());
		}
	}

	private LaserMetaqTask thread;
	private LaserFeatureListenser listener;

	private LaserMetaqThread() throws IOException {
		try {
			Configuration conf = Configuration.getInstance();
			org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
			Path metaqOutput = conf.getMetaqOutput();
			FileSystem fs = metaqOutput.getFileSystem(hadoopConf);

			listener = new LaserFeatureListenser(conf.getCouchbaseCluster(),
					conf.getCouchbaseBucket(), conf.getCouchbasePassword(),
					metaqOutput, fs, hadoopConf,
					conf.getItemFeatureDimension(),
					conf.getUserFeatureDimension());
			thread = new LaserMetaqTask();

		} catch (Exception e) {
			LOG.info(e.getMessage());
			throw new IOException(e.getCause());
		}
	}

	public void start() {
		thread.start();
	}

	public void exit() throws IOException {
		thread.exit();
	}

	public void incrMinorVersion() throws IOException {
		listener.incrMinorVersion();
	}

	public long getMinorVersion() {
		return listener.geMinorVersion();
	}

	public void incrMajorVersion() throws IOException {
		listener.incrMajorVersion();
	}

	public long getMajorVersion() {
		return listener.geMajorVersion();
	}
}
