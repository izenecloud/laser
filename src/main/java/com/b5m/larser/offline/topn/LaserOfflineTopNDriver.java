package com.b5m.larser.offline.topn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import com.b5m.couchbase.CouchbaseConfig;
import com.b5m.couchbase.CouchbaseInputFormat;
import com.b5m.msgpack.MsgpackOutputFormat;

public class LaserOfflineTopNDriver {
	public static int run(Integer topN, Configuration baseConf)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(baseConf);

		conf.set(CouchbaseConfig.CB_INPUT_CLUSTER, com.b5m.conf.Configuration
				.getInstance().getCouchbaseCluster());
		conf.set(CouchbaseConfig.CB_INPUT_BUCKET, com.b5m.conf.Configuration
				.getInstance().getCouchbaseBucket());
		conf.set(CouchbaseConfig.CB_INPUT_PASSWORD, com.b5m.conf.Configuration
				.getInstance().getCouchbasePassword());
		conf.setInt("laser.offline.topn.n", topN);
		conf.set("com.b5m.msgpack.ip", com.b5m.conf.Configuration.getInstance()
				.getMsgpackAddress());
		conf.setInt("com.b5m.msgpack.port", com.b5m.conf.Configuration
				.getInstance().getMsgpackPort());
		conf.set("com.b5m.msgpack.method", "updateTopNCluster");
		conf.set("com.b5m.laser.offline.topn.offline.model",
				com.b5m.conf.Configuration.getInstance()
						.getLaserOfflineOutput().toString());

		Job job = Job.getInstance(conf);
		job.setJarByClass(LaserOfflineTopNDriver.class);

		job.setInputFormatClass(CouchbaseInputFormat.class);
		job.setOutputFormatClass(MsgpackOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PriorityQueue.class);

		job.setMapperClass(LaserOfflineTopNMapper.class);
		job.setNumReduceTasks(0);
		
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
		return 0;
	}
}
