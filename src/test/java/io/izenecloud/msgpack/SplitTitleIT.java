package io.izenecloud.msgpack;

import io.izenecloud.conf.Configuration;
import io.izenecloud.msgpack.MsgpackClient;
import io.izenecloud.msgpack.SparseVector;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class SplitTitleIT {
	private static final String PROPERTIES = "src/test/properties/";

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
		Configuration conf = Configuration.getInstance();
		String collection = conf.getCollections().get(0);
		MsgpackClient client = new MsgpackClient(conf.getMsgpackAddress(collection),conf.getMsgpackPort(collection), collection);
		try {
			Object[] req = new Object[1];
			req[0] = "十二结婚吧 i [http://446964573.qzone.qq.com]";
			SparseVector res = (SparseVector) client.asyncRead(req, "splitTitle", SparseVector.class);
			while (res.hasNext()) {
				System.out.println(Integer.toString(res.getIndex()) + "\t" + res.get());
			}
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	@AfterTest
	public void close() throws UnknownHostException {
	}
}
