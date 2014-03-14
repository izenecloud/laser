package com.b5m.larser;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static com.b5m.larser.LaserOfflineHelper.*;
public class LaserOfflineTopNResult {
	private final List<List<DoubleIntPairWritable>> topN;
	public LaserOfflineTopNResult(Path path, FileSystem fs, Configuration conf) {
		topN = readTopNResult(path, fs, conf);
	}
	
	public List<DoubleIntPairWritable> get(int userId) {
		return topN.get(userId);
	}
}
