package com.b5m.larser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.b5m.larser.LaserOfflineHelper.*;

public class LaserFirstOrderDriver {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserFirstOrderDriver.class);

	public static int laserFirstOrder(Path itemFeatures, Path userFeatures,
			Path alpha, Path beta, Path output, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		Path itemRes = new Path(output, "first_order_item_res");
		LOG.info("Calculate item relatived part of first order, result = {}",
				itemRes);
		doLaserFirstOrder(itemFeatures, itemRes, beta, conf);
		FileSystem fs = itemRes.getFileSystem(conf);

		Path userResPath = new Path(output, "first_order_user_res");
		LOG.info("Calculate user relatived part of first order, result = {}",
				userResPath);
		FSDataInputStream in = fs.open(userFeatures);
		Matrix user  = MatrixWritable.readMatrix(in);
		in = fs.open(alpha);
		Vector alphas = VectorWritable.readVector(in);
		Vector userRes = new DenseVector(user.numRows());
		for (int row = 0; row < userRes.size(); row++) {
			userRes.set(row, user.viewRow(row).dot(alphas));
		}
		
		writeVector(userResPath, fs, conf, userRes);
		return 0;
	}

	public static int doLaserFirstOrder(Path features, Path output, Path args,
			Configuration baseConf) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration(baseConf);
		conf.set("laser.first.order.args", args.toString());
		DistributedCache.addCacheFile(args.toUri(), conf);

		Job job = new Job(conf);
		job.setJarByClass(LaserFirstOrderDriver.class);

		FileInputFormat.setInputPaths(job, features);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setMapperClass(LaserFirstOrderMapper.class);
		job.setReducerClass(LaserFirstOrderReducer.class);

		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
		return 0;
	}
}
