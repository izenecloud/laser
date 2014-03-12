package com.b5m.larser;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;


public class Alpha {
	private static final Integer D = 100;
	private static final Integer N = 10000;
	private static final Random random = new Random();
	
	public static void randomAlpha(Path path, FileSystem fs) throws IOException {
		Matrix alpha = new DenseMatrix(N, D);
		for (int i = 0; i < N; i++) {
			Vector row = alpha.viewRow(i);
			for (int j = 0; j < D; j++) {
				row.set(j, random.nextDouble());
			}
		}
		FSDataOutputStream out = fs.create(path);
		new MatrixWritable(alpha).write(out);
		out.close();
	}
}
