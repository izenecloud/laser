package com.b5m.lr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class LrIterationHelper {
	public static String createItemIdFromHdfsPath(Path path) {
		return Integer.toString(path.toString().hashCode());
	}

	public static Vector readVectorGivenIdentifier(Path path, FileSystem fs,
			int identifier, Configuration conf) {
		for (Pair<IntWritable, VectorWritable> row : new SequenceFileDirIterable<IntWritable, VectorWritable>(
				new Path(path, "part-*"), PathType.GLOB, conf)) {
			if (row.getFirst().get() == identifier)
				return row.getSecond().get();
		}
		return null;
	}
}
