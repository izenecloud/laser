package com.b5m.larser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public final class LaserOfflineHelper {
	
	public static Vector readVector(Path path, FileSystem fs, Configuration conf) {
		for (Pair<Writable, VectorWritable> row : new SequenceFileDirIterable<Writable, VectorWritable>(
				new Path(path, "part-*"), PathType.GLOB, conf)) {
			return row.getSecond().get();
		}
		return null;
	}
	
	public static void writeVector(Path path, FileSystem fs, Configuration conf, Vector v) throws IOException {
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(path, "part-r-00000"), NullWritable.class, VectorWritable.class);
		writer.append(NullWritable.get(), new VectorWritable(v));
		writer.close();
	}
}
