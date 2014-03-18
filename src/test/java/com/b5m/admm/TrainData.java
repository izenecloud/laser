package com.b5m.admm;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TrainData {
	private static final Integer NON_ZERO_PER_FEATURE = 100;
	private static final Integer FEATRUE_DIMENSION = 1000000; // 1M
	private static final Integer SMAPLE_DIMENSION = 1000000000 / 1000000000; // 1000M
	private static final Double CTR = 0.0005;

	private static final Random random = new Random();

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
}
