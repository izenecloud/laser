package com.b5m.larser.offline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

public class LaserFirstOrderMapper extends
		Mapper<IntWritable, VectorWritable, NullWritable, VectorWritable> {
	private Vector args;
	private Vector res;
	private static final Integer N = 10000000;


	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Path args = new Path(conf.get("laser.first.order.args"));
		FileSystem fs = args.getFileSystem(conf);
		FSDataInputStream input = fs.open(args);
		this.args = VectorWritable.readVector(input);
		res = new DenseVector(N);
	}

	protected void map(IntWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		double res = 0.0;
		for (Element e : value.get().nonZeroes()) {
			res += args.get(e.index()) * e.get();
		}
		this.res.set(key.get(), res);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		context.write(NullWritable.get(), new VectorWritable(res));
	}
}
