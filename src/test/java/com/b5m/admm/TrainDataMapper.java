package com.b5m.admm;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TrainDataMapper extends
		Mapper<Writable, Writable, IntWritable, VectorWritable> {
	private static final Integer NON_ZERO_PER_FEATURE = 100;
	private static final Integer FEATRUE_DIMENSION = 1000000; // 1M
	private static final Random random = new Random();

	private long samplePerMapTask;
	private Vector args;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Path args = new Path(conf.get("com.b5m.admm.train.data.args"));
		FileSystem fs = args.getFileSystem(conf);
		this.args = VectorWritable.readVector(fs.open(args));

		samplePerMapTask = conf.getLong("com.b5m.admm.sample.per.mapTask", 0);
	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		for (long i = 0; i < samplePerMapTask; i++) {
			Vector row = new SequentialAccessSparseVector(FEATRUE_DIMENSION + 1);
			for (int n = 0; n < NON_ZERO_PER_FEATURE; n++) {
				int index = random.nextInt() % FEATRUE_DIMENSION;
				if (0 > index) {
					index *= -1;
				}
				row.set(index, random.nextDouble() * args.getQuick(index));
			}
			if (random.nextInt() % 10000 < 5)
				row.set(FEATRUE_DIMENSION, 1);
			else
				row.set(FEATRUE_DIMENSION, -1);

			context.write(new IntWritable((int) i), new VectorWritable(row));
		}
	}
}
