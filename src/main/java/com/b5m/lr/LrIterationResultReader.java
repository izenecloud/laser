package com.b5m.lr;

import static com.b5m.lr.LrIterationHelper.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

public class LrIterationResultReader {
	private final Path res;
	private final Configuration conf;
	private final FileSystem fs;

	public LrIterationResultReader(Path path, FileSystem fs, Configuration conf) {
		this.res = path;
		this.fs = fs;
		this.conf = conf;
	}

	public Vector readResult(int identifier) {
		return readVectorGivenIdentifier(res, fs, identifier, conf);
	}
}
