package com.b5m.larser.dispatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestDispatch {
	private Dispatcher dispatcher;
	private Path properties;
	@BeforeTest
	public void setup() throws Exception {
		dispatcher = new Dispatcher();
		properties = new Path("src/test/properties/laser.properties");
		Component.setComponentBasePath(new Path("TEST"));
	}

	@AfterTest
	public void close() {
		
	}

	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = properties.getFileSystem(conf);
		dispatcher.run(properties, fs);
	}
}
