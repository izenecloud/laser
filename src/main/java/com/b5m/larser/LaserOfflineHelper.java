package com.b5m.larser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
	
	public static List<List<DoubleIntPairWritable>> readTopNResult(Path path, FileSystem fs, Configuration conf) {
		List<List<DoubleIntPairWritable>> topN = new ArrayList<List<DoubleIntPairWritable>>();
		for (Pair<IntWritable, PriorityQueueWritable> row : new SequenceFileDirIterable<IntWritable, PriorityQueueWritable>(
				new Path(path, "part-*"), PathType.GLOB, conf)) {
			int userId = row.getFirst().get();
			List<DoubleIntPairWritable> userTopN = new LinkedList<DoubleIntPairWritable>();

			PriorityQueue queue = row.getSecond().get();
			Iterator<DoubleIntPairWritable> iterator = queue.iterator();
			while (iterator.hasNext()) {
				userTopN.add(iterator.next());
			}
			topN.add(userId, userTopN);
		}
		return topN;
	}
}
