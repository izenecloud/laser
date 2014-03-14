package com.b5m.larser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.b5m.larser.offline.LaserFirstOrderDriver;
import com.b5m.larser.offline.LaserOfflineTopNDriver;
import com.b5m.larser.offline.LaserOfflineTopNResult;
import com.b5m.larser.offline.LaserSecondOrderDriver;

public class TestLaserOffline {
	private static final Logger LOG = LoggerFactory
			.getLogger(TestLaserOffline.class);

	Path itemFeatures;
	Path userCluster;
	Path a;
	Path alpha;
	Path beta;
	Path output;
	Configuration conf;
	FileSystem fs;

	@BeforeTest
	public void setup() throws Exception {
		itemFeatures = new Path("tmp/itemFeatures");
		userCluster = new Path("tmp/userCluster");
		a = new Path("tmp/a");
		alpha = new Path("tmp/alpha");
		beta = new Path("tmp/beta");
		output = new Path("tmp/output");
		conf = new Configuration();
		fs = itemFeatures.getFileSystem(conf);
		ItemFeature.randomSequence(itemFeatures, fs, conf);
		UserCluster.random(userCluster, fs);
		A.random(a, fs);
		Alpha.randomAlpha(alpha, fs);
		Betas.randomBetas(beta, fs);
	}

	@AfterTest
	public void close() {

	}

	@Test
	public void test() {
		try {
			LaserFirstOrderDriver.laserFirstOrder(itemFeatures, userCluster,
					alpha, beta, output, conf);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertFalse(true);
		}

		try {
			LaserSecondOrderDriver.laserSecondOrder(itemFeatures, userCluster,
					a, output, conf);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertFalse(true);
		}
		Path topN = new Path(output, "top_n");
		try {
			LaserOfflineTopNDriver.topN(new Path(output, "XAC"), output, topN,
					10, conf);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertFalse(true);
		}		
		LaserOfflineTopNResult topNResult = new LaserOfflineTopNResult(topN, fs, conf);
		Assert.assertTrue(null != topNResult);
	}

}
