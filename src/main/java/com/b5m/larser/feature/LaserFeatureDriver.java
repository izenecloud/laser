package com.b5m.larser.feature;

import static com.b5m.larser.feature.LaserFeatureHelper.deleteFiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

public class LaserFeatureDriver {

	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Configuration conf = new Configuration();
		LaserFeatureDriver.run(input, output, conf);
	}

	public static int run(Path input, Path output, Configuration baseConf)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(baseConf);

		Job job = new Job(conf);
		job.setJarByClass(LaserFeatureDriver.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(LaserFeatureInputFormat.class);
		job.setOutputFormatClass(LaserFeatureOutputFormat.class);

		job.setMapOutputKeyClass(IntLongPairWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(LaserFeatureMapper.class);
		job.setReducerClass(LaserFeatureReducer.class);
		;

		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}

		deleteFiles(output, new String("part-r-*"), output.getFileSystem(conf));
		return 0;
	}
}
