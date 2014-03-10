package com.b5m.larser.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger LOG = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path itemFeatures = new Path(args[0]);
		Path ap = new Path(args[1]);
		Path output = new Path(args[2]);
		Configuration conf = new Configuration();
		FileSystem fs = itemFeatures.getFileSystem(conf);
		
		//ItemFeature.random(itemFeatures, fs);
		//A.random(ap, fs);
		LOG.info("AC");
		LaserSecondOrderDriver.doLaserSecondOrderWitinItemFeaturs(itemFeatures, output, ap, conf);
		LOG.info("END");
	}

}
