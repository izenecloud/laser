package com.b5m.larser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Demo {
	private static final Logger LOG = LoggerFactory.getLogger(Demo.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path itemFeatures = new Path(args[0]);
		Path userCluster = new Path(args[1]);
		Path ap = new Path(args[2]);
		Path alpha = new Path(args[3]);
		Path beta = new Path(args[4]);
		Path output = new Path(args[5]);
		Configuration conf = new Configuration();
//		FileSystem fs = itemFeatures.getFileSystem(conf);		
//		ItemFeature.randomSequence(itemFeatures, fs, conf);
//		UserCluster.random(userCluster, fs);
//		A.random(ap, fs);
//		Alpha.randomAlpha(alpha, fs);
//		Betas.randomBetas(beta, fs);
//		LaserFirstOrderDriver firstOrderDriver = new LaserFirstOrderDriver();
//		firstOrderDriver.laserFirstOrder(itemFeatures, userCluster, alpha, beta, output, conf);
//		
//		LaserSecondOrderDriver.laserSecondOrder(itemFeatures, userCluster, ap, output, conf);
		
		LaserOfflineTopNDriver.topN(new Path(output, "XAC"),  output, new Path(output, "top_n"), 1000, 100000, conf);
	}

}
