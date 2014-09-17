package io.izenecloud.larser.online;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

public class LaserOnlineEta {
	private final Matrix eta;
	private static final int ITEM_DIMENSION = 1000000;
	private static final int USER_FEATURE_DIMENSION = 100;
	private static final Random random = new Random();
	
	public LaserOnlineEta(Path path, FileSystem fs, Configuration conf) {
		eta = new DenseMatrix(ITEM_DIMENSION, USER_FEATURE_DIMENSION);
		for (int row = 0; row < ITEM_DIMENSION; row ++) {
			Vector v = eta.viewRow(row);
			for (int col = 0; col < USER_FEATURE_DIMENSION; col++) {
				v.set(col, random.nextDouble());
			}
		}
	}
	
	public Vector getEta(int itemId) {
		return eta.viewRow(itemId);
	}
}
