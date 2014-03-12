package com.b5m.larser;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;


public class LaserSecondOrderDriver {
	private static final Pattern TAB_PATTERN = Pattern.compile("\t");
	
	public static int laserSecondOrder(Path itemFeatures,
			Path userFeatures, Path args, Path output, Configuration baseConf) throws IOException,
			ClassNotFoundException, InterruptedException {
		Path AC = new Path(output, "AC");
		doLaserSecondOrder(itemFeatures, AC, args, baseConf);
		Path XAC = new Path(output, "XAC");
		doLaserSecondOrder(AC, XAC, userFeatures, baseConf);
		return 0;
	}

	public static int doLaserSecondOrder(Path features,
			Path output, Path args, Configuration baseConf) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(baseConf);
		conf.set("laser.second.order.args", args.toString());
		DistributedCache.addCacheFile(args.toUri(), conf);

		Job job = new Job(conf);
		job.setJarByClass(LaserSecondOrderDriver.class);

		FileInputFormat.setInputPaths(job, features);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(LaserSecondOrderMapper.class);
		job.setNumReduceTasks(0);
		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
		return 0;
	}
	
	public static int doLaserSecondOrderWitinUserCluster(Path itemFeatures,
			Path output, Path args, Configuration baseConf) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(baseConf);
		conf.set("laser.second.order.a", args.toString());
		DistributedCache.addCacheFile(args.toUri(), conf);

		Job job = new Job(conf);
		job.setJarByClass(LaserSecondOrderDriver.class);

		FileInputFormat.setInputPaths(job, itemFeatures);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(LaserSecondOrderMapper.class);
		job.setReducerClass(LaserSecondOrderReducer.class);
		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
		return 0;
	}
	
	public static long topN(Path acPath, Configuration conf, List<Long> cadidateList, double[] user) throws IOException {
		AC ac = new AC(acPath,  conf);
		long sTime = System.nanoTime();
		java.util.Iterator<Long> it = cadidateList.iterator();
		while (it.hasNext()) {
			Long cadidate = it.next();
			double[] row = createArrayFromDataString(ac.readLine(cadidate));
			double res = 0.0;
			for (int i = 0; i < row.length; i++) {
				res += user[i] * row[i];
			}
		}
		return System.nanoTime() - sTime;
	}
	
	public static double[] createArrayFromDataString(String dataString) {
		System.out.println(dataString);
		String[] items = TAB_PATTERN.split(dataString);
		int n = items.length;
		double[] arr = new double[n];
		for (int i = 0; i < n; i++) {
			arr[i] = Double.valueOf(items[i]);
		}
		return arr;
	}
}
