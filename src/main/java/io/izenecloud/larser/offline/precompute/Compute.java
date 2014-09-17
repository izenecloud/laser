package io.izenecloud.larser.offline.precompute;

import io.izenecloud.msgpack.MsgpackInputFormat;
import io.izenecloud.msgpack.MsgpackOutputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Compute {
	
	public static int run(Path model, Configuration baseConf) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration(baseConf);
		conf.set("com.b5m.laser.msgpack.input.method", "ad_feature");
		conf.set("com.b5m.laser.msgpack.output.method", "precompute_ad_offline_model");
		conf.set("com.b5m.laser.offline.model", model.toString());
		Job job = Job.getInstance(conf);
		job.setJarByClass(Compute.class);
		job.setJobName("per compute stable part from offline model for each user");
		job.setInputFormatClass(MsgpackInputFormat.class);
		job.setOutputFormatClass(MsgpackOutputFormat.class);
		
		job.setOutputKeyClass(Long.class);
		job.setOutputValueClass(Result.class);
		
		job.setMapperClass(Mapper.class);
		job.setNumReduceTasks(0);
		
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}

		return 0;
	}
}
