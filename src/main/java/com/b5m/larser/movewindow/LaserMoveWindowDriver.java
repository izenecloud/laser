package com.b5m.larser.movewindow;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

import static com.b5m.larser.feature.LaserFeatureHelper.*;

public class LaserMoveWindowDriver {
	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Configuration conf = new Configuration();
		Calendar calendar = Calendar.getInstance();
		Date now = calendar.getTime();
		calendar.add(Calendar.YEAR, -1);
		LaserMoveWindowDriver.run(input, output, calendar.getTime(), now, conf);
	}

	public static int run(Path input, Path output, Date sTime, Date eTime,
			Configuration baseConf) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration(baseConf);
		conf.setLong("laser.move.window.start.time", sTime.getTime() / 1000);
		conf.setLong("laser.move.window.end.time", eTime.getTime() / 1000);
		Job job = new Job(conf);
		job.setJarByClass(LaserMoveWindowDriver.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(LaserMoveWindowOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(LaserMoveWindowMapper.class);
		job.setNumReduceTasks(0);

		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
		deleteFiles(output, new String("part-m-*"), output.getFileSystem(conf));

		return 0;
	}
}
