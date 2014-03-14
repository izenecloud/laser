package com.b5m.larser.offline;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;

public class A {
	private static final Integer COL = 500;
	private static final Integer ROW = 10;

	private static final Random random = new Random();
	
	public static void random(Path path, FileSystem fs) throws IOException {
		FSDataOutputStream output = fs.create(path);
		Matrix a = new DenseMatrix(ROW, COL);
		for (int i = 0; i < ROW; i++) {
			Vector v = a.viewRow(i);
			for (int j = 0; j < COL; j++) {
				v.set(j, random.nextDouble());
			}
		}
		new MatrixWritable(a).write(output);
		output.close();
	}
}
