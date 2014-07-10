package com.b5m.larser.framework;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.b5m.conf.Configuration;
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

	private Map<JobDetail, CronTrigger> triggersAndJobs = new HashMap<JobDetail, CronTrigger>();

	@SuppressWarnings("restriction")
	public void run() throws CmdLineException, IOException, SchedulerException,
			MetaClientException {

		final LaserMessageConsumeTask consumeTask = new LaserMessageConsumeTask();
		for (String collection : Configuration.getInstance().getCollections()) {
			try {
				startCollection(consumeTask, collection);
				LOG.info("collection {} has started", collection);
			} catch (Exception e) {
				LOG.error("error to start collection {}, reason: {}",
						collection, e.getCause());
				LOG.info("remove collecton {}", collection);
			}
		}
		try {
			LOG.info("start metaq task");
			consumeTask.start();
			LOG.info("start finish");
		} catch (Exception e) {
			LOG.error(e.getLocalizedMessage());
		}

		StdSchedulerFactory factory = new StdSchedulerFactory();
		// factory.createVolatileScheduler(10);
		final Scheduler scheduler = factory.getScheduler();
		scheduler.start();

		Iterator<Map.Entry<JobDetail, CronTrigger>> iterator = triggersAndJobs
				.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<JobDetail, CronTrigger> job = iterator.next();
			LOG.info(job.getKey().getKey().getName());
			LOG.info(job.getValue().getCronExpression());
			scheduler.scheduleJob(job.getKey(), job.getValue());
		}
		LOG.info("Laser Training Framework has started");

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

	private void startCollection(final LaserMessageConsumeTask consumeTask,
			String collection) throws SchedulerException, IOException,
			MetaClientException, ClassNotFoundException,
			InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
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

		try {
			JobKey key = new JobKey("online train task", collection);
			JobDetail laserOnlineTrainTask = JobBuilder
					.newJob(LaserOnlineTrainTask.class).withIdentity(key)
					.build();

			laserOnlineTrainTask.getJobDataMap().put(
					"com.b5m.laser.message.consumer", consumer);
			CronTrigger laserOnlineTrainTaskTrigger = TriggerBuilder
					.newTrigger()
					.withIdentity("online train task", collection)
					.withSchedule(
							CronScheduleBuilder.cronSchedule(Configuration
									.getInstance()
									.getLaserOnlineRetrainingFreqency(
											collection))).build();

			LOG.info(Configuration.getInstance()
					.getLaserOnlineRetrainingFreqency(collection));
			triggersAndJobs.put(laserOnlineTrainTask,
					laserOnlineTrainTaskTrigger);
			LOG.info("Laser Online Train Task add to scheduler's map");

		} catch (Exception e) {
			e.printStackTrace();
		}

		try {

			JobKey key = new JobKey("offline train task", collection);
			JobDetail laserOfflineTrainTask = JobBuilder
					.newJob(LaserOfflineTrainTask.class).withIdentity(key)
					.build();

			laserOfflineTrainTask.getJobDataMap().put(
					"com.b5m.laser.message.consumer", consumer);
			CronTrigger laserOfflineTrainTaskTrigger = TriggerBuilder
					.newTrigger()
					.withIdentity("offline train task", collection)
					.withSchedule(
							CronScheduleBuilder.cronSchedule(Configuration
									.getInstance()
									.getLaserOfflineRetrainingFreqency(
											collection))).build();

			LOG.info(Configuration.getInstance()
					.getLaserOfflineRetrainingFreqency(collection));
			triggersAndJobs.put(laserOfflineTrainTask,
					laserOfflineTrainTaskTrigger);
			LOG.info("Laser Offline Train Task add to scheduler' map");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void stopCollection(final Scheduler scheduler,
			final LaserMessageConsumeTask consumeTask, String collection)
			throws SchedulerException, MetaClientException, IOException {
		LOG.info("stop collection {}", collection);
		{
			JobKey key = new JobKey("online train task", collection);
			scheduler.deleteJob(key);
			LOG.info("delete {}'s online train task", collection);
		}
		{
			JobKey key = new JobKey("offline train task", collection);
			scheduler.deleteJob(key);
			LOG.info("delete {}'s offline train task", collection);
		}

	}
}
