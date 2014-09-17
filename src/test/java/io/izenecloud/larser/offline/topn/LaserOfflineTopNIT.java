package io.izenecloud.larser.offline.topn;

import static io.izenecloud.HDFSHelper.writeMatrix;
import static io.izenecloud.HDFSHelper.writeVector;
import io.izenecloud.conf.Configuration;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class LaserOfflineTopNIT {
	private static final String PROPERTIES = "src/test/properties/laser.properties.examble";
	private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
	private int topn = 3;

//	@BeforeTest
//	public void setup() throws IOException {
//		Path pro = new Path(PROPERTIES);
//		FileSystem fs = pro.getFileSystem(conf);
//		Configuration.getInstance().load(pro, fs);
//
//		Path output = Configuration.getInstance().getLaserOfflineOutput();
//		int rows = Configuration.getInstance().getUserFeatureDimension();
//		int cols = Configuration.getInstance().getItemFeatureDimension();
//		Vector alpha = new DenseVector(rows);
//		writeVector(alpha, new Path(output, "alpha"), fs, conf);
//		Vector beta = new DenseVector(cols);
//		writeVector(beta, new Path(output, "beta"), fs, conf);
//		Matrix A = new DenseMatrix(rows, cols);
//		writeMatrix(A, new Path(output, "A"), fs, conf);
//	}
//
//	@Test
//	public void topN() throws ClassNotFoundException, IOException,
//			InterruptedException {
//		LaserOfflineTopNDriver.run(topn, conf);
//	}
//
//	@AfterTest
//	public void close() {
//
//	}
}
