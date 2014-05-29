package com.b5m.larser.framework;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.DirectSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.b5m.conf.Configuration;
import com.b5m.larser.feature.GeneralMesseageConsumer;
import com.b5m.larser.feature.LaserMessageConsumer;
import com.taobao.metamorphosis.exception.MetaClientException;

public class Laser {

	public static void main(String[] args) throws CmdLineException,
			IOException, SchedulerException, MetaClientException {
		LaserArgument.parseArgs(args);
		Laser framework = new Laser();
		framework.run();
	}

	private static final Logger LOG = LoggerFactory.getLogger(Laser.class);

	@SuppressWarnings("restriction")
	public void run() throws CmdLineException, IOException, SchedulerException,
			MetaClientException {
		DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();
		factory.createVolatileScheduler(10);
		final Scheduler scheduler = factory.getScheduler();
		final LaserMessageConsumeTask consumeTask = new LaserMessageConsumeTask();
		for (String collection : Configuration.getInstance().getCollections()) {
			try {
				startCollection(scheduler, consumeTask, collection);
			} catch (Exception e) {
				LOG.error("error to start collection {}, reason: {}",
						collection, e.getCause());
				LOG.info("remove collecton {}", collection);
			}
		}

		consumeTask.start();
		scheduler.start();
		LOG.info("Laser Training Frameworh has started");

		Signal.handle(new Signal("INT"), new SignalHandler() {
			public void handle(Signal sig) {
				consumeTask.stop();

				for (String collection : Configuration.getInstance()
						.getCollections()) {
					try {
						stopCollection(scheduler, consumeTask, collection);
					} catch (SchedulerException e) {
						e.printStackTrace();
					} catch (MetaClientException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					e.printStackTrace();
				}
				System.exit(0);
			}
		});
	}

	private void startCollection(final Scheduler scheduler,
			final LaserMessageConsumeTask consumeTask, String collection)
			throws SchedulerException, IOException, MetaClientException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {
		LOG.info("start collection {}", collection);
		Path messageOutput = Configuration.getInstance().getMetaqOutput(
				collection);
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		FileSystem fs = messageOutput.getFileSystem(conf);
		LaserMessageConsumer consumer = null;

		Class<? extends LaserMessageConsumer> consumerClass = Configuration
				.getInstance().getMessageConsumer(collection);

		consumer = consumerClass.getConstructor(String.class, Path.class,
				FileSystem.class, org.apache.hadoop.conf.Configuration.class)
				.newInstance(collection, messageOutput, fs, conf);

		consumeTask.addTask(collection, consumer);

		LOG.info("Laser Message Consume Task, output = {}", messageOutput);

		JobDetail laserOnlineTrainTask = new JobDetail("online train task",
				collection, LaserOnlineTrainTask.class);
		laserOnlineTrainTask.getJobDataMap().put(
				"com.b5m.laser.message.consumer", consumer);
		CronTrigger laserOnlineTrainTaskTrigger = new CronTrigger(
				"online train task", collection);
		try {
			CronExpression cexp = new CronExpression(Configuration
					.getInstance().getLaserOnlineRetrainingFreqency(collection));
			laserOnlineTrainTaskTrigger.setCronExpression(cexp);
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOG.info("Laser Online Train Task add to scheduler");
		scheduler
				.scheduleJob(laserOnlineTrainTask, laserOnlineTrainTaskTrigger);

		JobDetail laserOfflineTrainTask = new JobDetail("offline train task",
				collection, LaserOnlineTrainTask.class);
		laserOfflineTrainTask.getJobDataMap().put(
				"com.b5m.laser.message.consumer", consumer);

		CronTrigger laserOfflineTrainTaskTrigger = new CronTrigger(
				"offline train task", collection);
		try {
			CronExpression cexp = new CronExpression(Configuration
					.getInstance()
					.getLaserOfflineRetrainingFreqency(collection));
			laserOfflineTrainTaskTrigger.setCronExpression(cexp);
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOG.info("Laser Offline Train Task add to scheduler");

		scheduler.scheduleJob(laserOfflineTrainTask,
				laserOfflineTrainTaskTrigger);
	}

	public void stopCollection(final Scheduler scheduler,
			final LaserMessageConsumeTask consumeTask, String collection)
			throws SchedulerException, MetaClientException, IOException {
		LOG.info("stop collection {}", collection);

		consumeTask.removeTask(collection);
		LOG.info("remove {}'s Message Consume Task ", collection);

		scheduler.deleteJob("online train task", collection);
		LOG.info("delete {}'s online train task", collection);

		scheduler.deleteJob("offline train task", collection);
		LOG.info("delete {}'s offline train task", collection);

	}
}
