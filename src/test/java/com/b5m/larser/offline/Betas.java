package com.b5m.larser.offline;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class Betas {
	private double[] betas;
	private static final Integer D = 500;
	private static final Random random = new Random();
	public Betas() {
		betas = new double[D];
	}
	
	public static Betas randomBetas() {
		Betas b = new Betas();
		for (int i = 0; i < D; i++) {
			b.betas[i] = random.nextDouble();
		}
		return b;
	}
	
	public static void randomBetas(Path path, FileSystem fs) throws IOException {
		Vector betas = new DenseVector(D);
		for (int i = 0; i < D; i++) {
			betas.set(i, random.nextDouble());
		}
		FSDataOutputStream out = fs.create(path);
		new VectorWritable(betas).write(out);
		out.close();
	}
}
