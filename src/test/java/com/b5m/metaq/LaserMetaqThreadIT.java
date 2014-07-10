package com.b5m.metaq;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.b5m.conf.Configuration;
import com.taobao.metamorphosis.exception.MetaClientException;

public class LaserMetaqThreadIT {
	private static final String PROPERTIES = "src/test/properties/laser.properties.examble";
//	private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
//	private FileSystem fs;
//	private LaserMetaqThread metaq;
//
//	@BeforeTest
//	public void setup() throws IOException, MetaClientException {
//		Path path = new Path(PROPERTIES);
//		fs = path.getFileSystem(conf);
//		Configuration.getInstance().load(path, fs);
//		metaq = LaserMetaqThread.getInstance();
//	}
//
//	@Test
//	public void test() throws IOException, URISyntaxException,
//			MetaClientException, InterruptedException {
//		metaq.start();
//		Thread.sleep(100000);
//		metaq.exit();
//		Path metaqOutput = Configuration.getInstance().getMetaqOutput();
//		assertTrue(fs.exists(metaqOutput));
//	}
//
//	@AfterTest
//	public void cleanup() throws IOException {
//		metaq.exit();
//	}

}
