package com.b5m.admm;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

public class TrainData {
	private static final Integer NON_ZERO_PER_FEATURE = 100;
	private static final Integer FEATRUE_DIMENSION = 1000000; // 1M
	private static final Integer SMAPLE_DIMENSION = 1000000; // 1000M
	private static final Double CTR = 0.0005;

	private static final Random random = new Random();

	public static void random(Path path, FileSystem fs) throws IOException {
		FSDataOutputStream output = fs.create(path);
		Vector arg = new RandomAccessSparseVector(FEATRUE_DIMENSION);
		for (int i = 0; i < FEATRUE_DIMENSION; i++) {
			arg.set(i, random.nextDouble());
		}

		for (int i = 0; i < SMAPLE_DIMENSION; i++) {
			Vector row = new SequentialAccessSparseVector(FEATRUE_DIMENSION + 1);
			for (int n = 0; n < NON_ZERO_PER_FEATURE; n++) {
				int index = random.nextInt() % FEATRUE_DIMENSION;
				if (0 > index) {
					index *= -1;
				}
				row.set(index, random.nextDouble() * arg.getQuick(index));
			}
			if (random.nextInt() % 10000 < 5)
				row.set(FEATRUE_DIMENSION, 1);
			else
				row.set(FEATRUE_DIMENSION, -1);

			String rowString = new String();
			for (Element e : row.nonZeroes()) {
				rowString += Integer.toString(e.index()) + ":"
						+ Double.toString(e.get()) + "\t";
			}
			new Text(rowString).write(output);
			if (i % 10000 == 0)
				System.out.println(i);
		}
		output.close();
	}

	static public void randomSequence(Path path, FileSystem fs,
			Configuration conf) throws IOException {
		@SuppressWarnings("deprecation")
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
				IntWritable.class, VectorWritable.class);

		Vector arg = new RandomAccessSparseVector(FEATRUE_DIMENSION);
		for (int i = 0; i < FEATRUE_DIMENSION; i++) {
			arg.set(i, random.nextDouble());
		}

		for (int i = 0; i < SMAPLE_DIMENSION; i++) {
			Vector row = new SequentialAccessSparseVector(FEATRUE_DIMENSION + 1);
			for (int n = 0; n < NON_ZERO_PER_FEATURE; n++) {
				int index = random.nextInt() % FEATRUE_DIMENSION;
				if (0 > index) {
					index *= -1;
				}
				row.set(index, random.nextDouble() * arg.getQuick(index));
			}
			if (random.nextInt() % 10000 < 5)
				row.set(FEATRUE_DIMENSION, 1);
			else
				row.set(FEATRUE_DIMENSION, -1);

			writer.append(new IntWritable(i), new VectorWritable(row));
			if (i % 10000 == 0)
				System.out.println(i);
		}
		writer.close();
	}

	static public void randomSequence(Path args, Path output, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		FileSystem fs = args.getFileSystem(conf);
		FSDataOutputStream out = fs.create(args);
		Vector arg = new RandomAccessSparseVector(FEATRUE_DIMENSION);
		for (int i = 0; i < FEATRUE_DIMENSION; i++) {
			arg.set(i, random.nextDouble());
		}
		new VectorWritable(arg).write(out);
		out.close();

		conf.set("com.b5m.admm.train.data.args", args.toString());
		conf.setLong("com.b5m.admm.sample.dimension", SMAPLE_DIMENSION);
		conf.setInt("com.b5m.admm.num.mapTasks", 240);

		Job job = new Job(conf);
		job.setJarByClass(TrainData.class);

		job.setInputFormatClass(TrainDataInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, args);
		FileOutputFormat.setOutputPath(job, output);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(TrainDataMapper.class);
		job.setNumReduceTasks(0);

		boolean isSuccess = job.waitForCompletion(true);
		if (!isSuccess) {

		}
	}
}
