package com.b5m.msgpack;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.b5m.conf.Configuration;
import com.b5m.larser.feature.SplitTitleRequest;
import com.b5m.larser.feature.SplitTitleResponse;

public class TestSplitTitle {
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
		SplitTitleRequest req = new SplitTitleRequest("中华 人民 共和国"); 
		SplitTitleResponse res = RpcClient.getInstance().spliteTitle(req);
		System.out.println(res.getResponse().getClass());
		
		PriorityQueue queue = new PriorityQueue("xxx", new com.b5m.larser.offline.topn.PriorityQueue());
		RpcClient.getInstance().updateTopNCluster(queue);
		
		EventLoop loop = EventLoop.defaultEventLoop();
		Client client = null;
		try {
			client = new Client(Configuration.getInstance().getMsgpackAddress(),Configuration.getInstance().getMsgpackPort(), loop);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		Object[] params = new Object[1];
		params[0] = queue;
		client.callApply("updateTopNCluster", params);
	}

	@AfterTest
	public void close() throws UnknownHostException {
		RpcClient.getInstance().close();
	}
}
