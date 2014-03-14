package com.b5m.larser.online;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import static com.b5m.larser.offline.LaserOfflineHelper.*;
public class LaserOnlineDelta {
	private final Vector delta;
	private static final int ITEM_DIMENSION = 1000000;
	private static final Random random = new Random();
	public LaserOnlineDelta(Path path, FileSystem fs, Configuration conf) {
		//delta = readVector(path, fs, conf);
		delta = new DenseVector(ITEM_DIMENSION);
		for (int col = 0; col < ITEM_DIMENSION; col++) {
			delta.set(col, random.nextDouble());
		}
	}
	
	public Vector get() {
		return delta;
	}
}
