package com.b5m;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


public final class HDFSHelper {
	public static Vector readVector(Path path, FileSystem fs, Configuration conf)
			throws IOException {
		FSDataInputStream in = fs.open(path);
		return VectorWritable.readVector(in);
	}

	public static void writeVector(Vector v, Path path, FileSystem fs,
			Configuration conf) throws IOException {
		FSDataOutputStream out = fs.create(path);
		VectorWritable.writeVector(out, v);
	}

	public static Matrix readMatrix(Path path, FileSystem fs, Configuration conf)
			throws IOException {
		FSDataInputStream in = fs.open(path);
		return MatrixWritable.readMatrix(in);
	}

	public static void writeMatrix(Matrix m, Path path, FileSystem fs,
			Configuration conf) throws IOException {
		FSDataOutputStream out = fs.create(path);
		MatrixWritable.writeMatrix(out, m);
	}
}
