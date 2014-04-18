package com.b5m.msgpack;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.b5m.conf.Configuration;

public class ClusterInfosIT {
	private static final String PROPERTIES = "src/test/properties/laser.properties.examble";

	@BeforeTest
	public void setup() throws IOException {
		Path pro = new Path(PROPERTIES);
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

		FileSystem fs = pro.getFileSystem(conf);
		Configuration.getInstance().load(pro, fs);
	}

	@Test
	public void test() throws ClassNotFoundException, IOException,
			InterruptedException {
		ClusterInfoRequest req = new ClusterInfoRequest();
		assertTrue(null != RpcClient.getInstance().getClusterInfos(req));
	}

	@AfterTest
	public void close() throws UnknownHostException {
		RpcClient.getInstance().close();
	}
}
