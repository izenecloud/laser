package com.b5m.larser;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class Alpha {
	private static final Integer D = 100;
	private static final Random random = new Random();

	public static void randomAlpha(Path path, FileSystem fs) throws IOException {
		Vector alpha = new DenseVector(D);

		for (int j = 0; j < D; j++) {
			alpha.set(j, random.nextDouble());
		}
		FSDataOutputStream out = fs.create(path);
		new VectorWritable(alpha).write(out);
		out.close();
	}
}
