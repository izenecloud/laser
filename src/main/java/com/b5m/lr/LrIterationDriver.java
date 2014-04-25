package com.b5m.lr;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.b5m.msgpack.MsgpackOutputFormat;

public class LrIterationDriver {
	public static int run(Path input, Path output, Float regularizationFactor,
			Boolean addIntercept, Configuration baseConf) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(baseConf);
		if (null != addIntercept) {
			conf.setBoolean("lr.iteration.add.intercept", addIntercept);
		}
		if (null != regularizationFactor) {
			conf.setDouble("lr.iteration.regulariztion.factor",
					regularizationFactor);
		}
		conf.set("com.b5m.msgpack.ip", com.b5m.conf.Configuration.getInstance()
				.getMsgpackAddress());
		conf.setInt("com.b5m.msgpack.port", com.b5m.conf.Configuration
				.getInstance().getMsgpackPort());
		conf.set("com.b5m.msgpack.method", "updateLaserOnlineModel");

		Job job = Job.getInstance(conf);
		job.setJarByClass(LrIterationDriver.class);
		job.setJobName("logistic regression");

		FileInputFormat.setInputPaths(job, input);

		job.setOutputFormatClass(MsgpackOutputFormat.class);
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(List.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(LrIterationMapper.class);
		job.setNumReduceTasks(0);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job:logistic regression,  Failed!");
		}
		return 0;
	}
}
