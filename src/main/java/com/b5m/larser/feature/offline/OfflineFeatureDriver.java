package com.b5m.larser.feature.offline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.HDFSHelper;

public class OfflineFeatureDriver {
	private static final Logger LOG = LoggerFactory
			.getLogger(OfflineFeatureDriver.class);

	public static int run(Path input, Path output, Configuration baseConf)
			throws IOException, ClassNotFoundException, InterruptedException {
		LOG.info("Calculating Laser's Offline Features...");
		Configuration conf = new Configuration(baseConf);
		Job job = new Job(conf);

		job.setJarByClass(OfflineFeatureDriver.class);
		job.setJobName("Calculate Laser's Offline Features");

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(OfflineFeatureMapper.class);
		job.setNumReduceTasks(0);
//		job.setCombinerClass(Reducer.class);
//		job.setReducerClass(Reducer.class);
		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}

		LOG.info("Deleting files: {}", input);
		HDFSHelper.deleteFiles(input.getParent(), input.getName(),
				input.getFileSystem(conf));
		return 0;
	}
}
