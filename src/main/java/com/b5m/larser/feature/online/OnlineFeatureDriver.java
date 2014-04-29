package com.b5m.larser.feature.online;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

import com.b5m.lr.ListWritable;

public class OnlineFeatureDriver {
	public static int run(Path input, Path output, Configuration baseConf)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(baseConf);
		conf.set("com.b5m.laser.online.feature.offline.model",
				com.b5m.conf.Configuration.getInstance()
						.getLaserOfflineOutput().toString());
		Job job = Job.getInstance(conf);

		job.setJarByClass(OnlineFeatureDriver.class);
		job.setJobName("Group each user's feature");

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ListWritable.class);

		job.setMapperClass(OnlineFeatureMapper.class);
		job.setReducerClass(OnlineFeatureReducer.class);

		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job:Group each user's feature,  Failed!");
		}

		return 0;
	}
}
