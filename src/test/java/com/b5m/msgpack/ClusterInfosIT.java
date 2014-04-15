package com.b5m.msgpack;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.msgpack.MessagePack;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.type.Value;
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
		ClusterInfoResponse res_ = RpcClient.getInstance().getClusterInfos(req);
		assertTrue(null != RpcClient.getInstance().getClusterInfos(req));

		EventLoop loop = EventLoop.defaultEventLoop();
		Client client = null;
		try {
			client = new Client(
					Configuration.getInstance().getMsgpackAddress(),
					Configuration.getInstance().getMsgpackPort(), loop);
			client.setRequestTimeout(10000);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		Object[] args = new Object[1];
		args[0] = new ClusterInfoRequest(1000025558);

		MessagePack msgpack = new MessagePack();
		msgpack.register(ClusterInfoResponse.class);
		
		Value res = client.callApply("getClusteringInfos", args);
		org.msgpack.unpacker.Converter convert = new org.msgpack.unpacker.Converter(
				msgpack, res);
		ClusterInfoResponse response = convert.read(ClusterInfoResponse.class);
	}

	@AfterTest
	public void close() throws UnknownHostException {
		RpcClient.getInstance().close();
	}
}
