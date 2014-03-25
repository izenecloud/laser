package com.b5m.larser.online;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import com.b5m.lr.ListWritable;

public class OnlineTrainData {
	private static final Integer ITEM_DIMENSION = 10000; // 10k
	private static final Integer NON_ZERO_PER_FEATURE = 100;
	private static final Integer FEATRUE_DIMENSION = 1000000; // 1M
	private static final Integer SMAPLE_DIMENSION_PER_ITEM = 1000; // 1K

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Configuration conf = new Configuration();
		conf.setInt("mapred.task.timeout", 6000000);
		conf.set("mapred.job.queue.name", "sf1");

		randomSequence(input, output, conf);
	}

	static public void randomSequence(Path input, Path output,
			Configuration conf) throws IOException, ClassNotFoundException,
			InterruptedException {
		FileSystem fs = input.getFileSystem(conf);
		FSDataOutputStream out = fs.create(input);
		out.close();

		conf.setInt("com.b5m.laser.online.feature.dimension", FEATRUE_DIMENSION);
		conf.setInt("com.b5m.laser.online.sample.dimension.per.item",
				SMAPLE_DIMENSION_PER_ITEM);
		conf.setInt("com.b5m.laser.online.item.dimension", ITEM_DIMENSION);
		conf.setInt("com.b5m.laser.online.none.zero.per.feature",
				NON_ZERO_PER_FEATURE);
		conf.setInt("com.b5m.laser.online.num.mapTasks", 240);

		Job job = new Job(conf);
		job.setJarByClass(OnlineTrainData.class);

		job.setInputFormatClass(OnlineTrainInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ListWritable.class);

		job.setMapperClass(OnlineTrainMapper.class);
		job.setNumReduceTasks(0);

		HadoopUtil.delete(conf, output);

		boolean isSuccess = job.waitForCompletion(true);
		if (!isSuccess) {

		}
	}
}
