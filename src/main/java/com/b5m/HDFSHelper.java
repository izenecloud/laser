package com.b5m;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.GlobFilter;
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

	public static void deleteFiles(Path path, String filePattern, FileSystem fs)
			throws IOException {
		FileStatus[] fileStatus = fs.listStatus(path, new GlobFilter(
				filePattern));
		for (FileStatus file : fileStatus) {
			fs.delete(file.getPath());
		}
	}

	public static Path[] getFilePaths(Configuration conf, FileSystem fs,
			Path filePath) throws IOException {
		FileStatus[] hdfsFiles = fs.listStatus(filePath);
		Path[] hdfsFilePaths = FileUtil.stat2Paths(hdfsFiles);
		List<Path> files = new ArrayList<Path>();
		for (Path hdfsFilePath : hdfsFilePaths) {
			FileStatus fileStatus = fs.getFileStatus(hdfsFilePath);
			if (!fileStatus.isDir()) {
				files.add(hdfsFilePath);
			}
		}
		return files.toArray(new Path[0]);
	}

}
