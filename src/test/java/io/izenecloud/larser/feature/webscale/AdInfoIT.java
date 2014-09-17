package io.izenecloud.larser.feature.webscale;

import static org.testng.Assert.assertTrue;
import io.izenecloud.conf.Configuration;
import io.izenecloud.larser.feature.webscale.AdInfo;
import io.izenecloud.msgpack.MsgpackClient;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AdInfoIT {
	private static final String PROPERTIES = "src/test/properties/";

	@BeforeTest
	public void setup() throws IOException {
		Path pro = new Path(PROPERTIES);
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

		FileSystem fs = pro.getFileSystem(conf);
		Configuration.getInstance().load(pro, fs);
	}

	@Test
	public void getAdInfoById() throws ClassNotFoundException, IOException,
			InterruptedException {
		Configuration conf = Configuration.getInstance();
		String collection = conf.getCollections().get(0);
		MsgpackClient client = new MsgpackClient(
				conf.getMsgpackAddress(collection),
				conf.getMsgpackPort(collection), collection);
		try {
			Object[] req = new Object[1];
			req[0] = "b5fb08cb419fb83e1d5310ea7a94d5dd";
			AdInfo adInfo = (AdInfo) client.asyncRead(req, "getAdInfoById",
					AdInfo.class);
			assertTrue(!adInfo.DOCID.isEmpty());
			assertTrue(!adInfo.clusteringId.isEmpty());
			while (adInfo.context.hasNext()) {
				System.out.println("(" + Integer.toString(adInfo.context.getIndex()) + "," + Float.toString(adInfo.context.get()) + ")");
			}			

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

}
