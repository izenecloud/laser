package io.izenecloud.larser.feature.online;

import io.izenecloud.larser.feature.OnlineVectorWritable;
import io.izenecloud.lr.ListWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnlineFeatureDriver {
	private static final Logger LOG = LoggerFactory
			.getLogger(OnlineFeatureDriver.class);

	public static long run(String collection, Path input, Path output,
			Configuration baseConf) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration(baseConf);
		Job job = Job.getInstance(conf);

		job.setJarByClass(OnlineFeatureDriver.class);
		job.setJobName("GROUP each record's feature BY identifier");

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OnlineVectorWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ListWritable.class);

		job.setMapperClass(OnlineFeatureMapper.class);
		job.setReducerClass(OnlineFeatureReducer.class);

		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job:Group feature,  Failed!");
		}
		Counter counter = job.getCounters().findCounter(
				"org.apache.hadoop.mapred.Task$Counter",
				"REDUCE_OUTPUT_RECORDS");
		long reduceOutputRecords = counter.getValue();

		LOG.info(
				"Job: GROUP each record's feature BY identifier, output recordes = {}",
				reduceOutputRecords);

		return reduceOutputRecords;
	}
}
