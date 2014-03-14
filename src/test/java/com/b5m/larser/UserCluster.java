package com.b5m.larser;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class UserCluster {
	private static final Integer ROW = 100;
	private static final Integer COL = 10;

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
	
	static public void randomSequence(Path path, FileSystem fs, Configuration conf) throws IOException {
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, VectorWritable.class);
		for (int i = 0; i < ROW; i++) {
			Vector row = new DenseVector(COL);
			for (int j = 0; j < COL; j++) {			
				row.set(j, random.nextDouble());
			}
			writer.append(new IntWritable(i), new VectorWritable(row));
		}
		writer.close();
	}
}
