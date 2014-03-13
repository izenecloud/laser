package com.b5m.larser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

public class LaserOfflineTopNDriver {
	public static int topN(Path secondOrderRes,
			Path firstOrderRes, Path output, Integer topN, Integer itemDimension, Configuration baseConf) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(baseConf);
		Path itemRes = new Path(firstOrderRes, "first_order_item_res");
		Path userRes = new Path(firstOrderRes, "first_order_user_res");
		conf.set("laser.offline.topN.driver.first.order.item.res", itemRes.toString());
		conf.set("laser.offline.topN.driver.first.order.user.res", userRes.toString());
		conf.setInt("laser.offline.topN.driver.top.n", topN);
		conf.setInt("laser.offline.topN.driver.item.dimension", itemDimension);
		
		Job job = new Job(conf);
		job.setJarByClass(LaserFirstOrderDriver.class);

		FileInputFormat.setInputPaths(job, secondOrderRes);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntDoublePairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		
		job.setMapperClass(LaserOfflineTopNMapper.class);
		//TODO Combiner is need or not based on test
		job.setCombinerClass(LaserOfflineTopNCombiner.class);
		job.setReducerClass(LaserOfflineTopNReducer.class);

		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
		return 0;
	}
}
