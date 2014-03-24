package com.b5m.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TestJVM {
	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);

		Path output = new Path(args[1]);
		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "sf1");
		conf.setInt("com.b5m.admm.num.mapTasks", 960);

		FileSystem fs = input.getFileSystem(conf);
		fs.create(input).close();

		Job job = new Job(conf);
		job.setJarByClass(TestJVM.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(TrainDataInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(TestJVMMap.class);
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
	}
}
