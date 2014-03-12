package com.b5m.larser;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ItemFeature {
	private static final Integer D = 500000;
	private static final Integer N = 10000000;
	private static final Integer NON_ZERO_N = 3;
	private static final Random random = new Random();
	
	static public void random(Path path, FileSystem fs) throws IOException {
		FSDataOutputStream output = fs.create(path);
		for (int i = 0; i < N; i++) {
			String feature = new String();
			for (int n = 0; n < NON_ZERO_N; n++) {
				int index = random.nextInt() / D;
				if (0 > index) {
					index *= -1;
				}
				feature += Integer.toString(index) + ":" + Double.toString(random.nextDouble()) + "\t";				
			}
			output.writeBytes(feature + "\n");
			if (i % 10000 == 0) {
				System.out.println(i);
			}
		}
		output.close();
	}
	
	static public void randomSequence(Path path, FileSystem fs, Configuration conf) throws IOException {
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, VectorWritable.class);
		for (int i = 0; i < N; i++) {
			Vector row = new RandomAccessSparseVector(D);
			for (int n = 0; n < NON_ZERO_N; n++) {
				int index = random.nextInt() / D;
				if (0 > index) {
					index *= -1;
				}
				row.set(index, random.nextDouble());
			}
			if (i % 10000 == 0) {
				System.out.println(i);
			}
			writer.append(new IntWritable(i), new VectorWritable(row));
		}
		writer.close();
	}
}
