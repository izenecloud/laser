package com.b5m.lr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

public class LrIterationDriver {
	public static int run(Path input, Path output, double regularizationFactor,
			boolean addIntercept, Configuration baseConf) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(baseConf);
		conf.setBoolean("lr.iteration.add.intercept", addIntercept);
		conf.setDouble("lr.iteration.regulariztion.factor",
				regularizationFactor);
		Job job = new Job(conf);
		job.setJarByClass(LrIterationDriver.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LrIterationMapContextWritable.class);

		job.setMapperClass(LrIterationMapper.class);
		job.setNumReduceTasks(0);

		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
		return 0;
	}
}
