package com.b5m.larser.framework;

import java.io.IOException;

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
import com.taobao.metamorphosis.exception.MetaClientException;

public class Laser {

	public static void main(String[] args) throws CmdLineException,
			IOException, SchedulerException, MetaClientException {
		LaserArgument.parseArgs(args);
		Laser framework = new Laser();
		framework.run();
	}

	private static final Logger LOG = LoggerFactory.getLogger(Laser.class);

	public void run() throws CmdLineException, IOException, SchedulerException,
			MetaClientException {
		DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();
		factory.createVolatileScheduler(10);
		final Scheduler scheduler = factory.getScheduler();

		for (String collection : Configuration.getInstance().getCollections()) {
			System.out.println(collection);
			try {
				startCollection(scheduler, collection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		scheduler.start();

		Signal.handle(new Signal("INT"), new SignalHandler() {
			public void handle(Signal sig) {

				for (String collection : Configuration.getInstance()
						.getCollections()) {
					try {
						stopCollection(scheduler, collection);
					} catch (SchedulerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MetaClientException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.exit(0);
			}
		});
	}

	private void startCollection(final Scheduler scheduler, String collection)
			throws SchedulerException, IOException, MetaClientException {
		LOG.info("start collection {}", collection);
		LaserMessageConsumeTask cosumeTask = LaserMessageConsumeTask
				.getInstance();
		Path messageOutput = Configuration.getInstance().getMetaqOutput(
				collection);
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		FileSystem fs = messageOutput.getFileSystem(conf);
		cosumeTask.addTask(collection, new GeneralMesseageConsumer(collection,
				messageOutput, fs, conf));

		LOG.info("Laser Message Consume Task, output = {}", messageOutput);

		JobDetail laserOnlineTrainTask = new JobDetail("online train task",
				collection, LaserOnlineTrainTask.class);
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

	public void stopCollection(final Scheduler scheduler, String collection)
			throws SchedulerException, MetaClientException, IOException {
		LOG.info("stop collection {}", collection);
		LaserMessageConsumeTask cosumeTask = LaserMessageConsumeTask
				.getInstance();
		cosumeTask.removeTask(collection);
		LOG.info("remove {}'s Message Consume Task ", collection);

		scheduler.deleteJob("online train task", collection);
		LOG.info("delete {}'s online train task", collection);

		scheduler.deleteJob("offline train task", collection);
		LOG.info("delete {}'s offline train task", collection);

	}
}
