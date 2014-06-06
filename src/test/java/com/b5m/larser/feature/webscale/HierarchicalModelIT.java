package com.b5m.larser.feature.webscale;

import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Vector;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.b5m.conf.Configuration;
import com.b5m.msgpack.MsgpackClient;

public class HierarchicalModelIT {
	private static final String PROPERTIES = "src/test/properties/";
	private MsgpackClient client;
	@BeforeTest
	public void setup() throws IOException {
		{
			Path pro = new Path(PROPERTIES);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

			FileSystem fs = pro.getFileSystem(conf);
			Configuration.getInstance().load(pro, fs);
		}
		Configuration conf = Configuration.getInstance();
		String collection = conf.getCollections().get(0);
		client = new MsgpackClient(
				conf.getMsgpackAddress(collection),
				conf.getMsgpackPort(collection), collection);
	}
	
	@Test
	public void updatePerAdOnlineModel() {
		try {
			Object[] req = new Object[3];
			req[0] = "2";
			req[1] = 0.5;
			req[2] = new Vector<Float>(400);
			Boolean ret = (Boolean) client.write(req, "updatePerAdOnlineModel",
					Boolean.class);
			assertTrue(ret);

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void updatePerClusteringModel() {
		try {
			Object[] req = new Object[3];
			req[0] = "1284|clustering";
			req[1] = 0.5;
			req[2] = new Vector<Float>(400);
			Boolean ret = (Boolean) client.write(req, "updatePerClusteringModel",
					Boolean.class);
			assertTrue(ret);

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void updateOfflineModel() {
		try {
			Object[] req = new Object[3];
			req[0] = new Vector<Float>(400);
			req[1] = new Vector<Float>(10000);
			Vector<Vector<Float>> quadratic = new Vector<Vector<Float>>(400);
			for (int i = 0; i < quadratic.size(); i++) {
				quadratic.set(i, new Vector<Float>(10000));
			}
			req[2] = quadratic;
			Boolean ret = (Boolean) client.write(req, "updateOfflineModel",
					Boolean.class);
			assertTrue(ret);

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	@AfterTest
	public void close() {
		client.close();
	}
}
