package com.b5m.msgpack;

import java.net.UnknownHostException;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;

import com.b5m.conf.Configuration;

public class RpcClient {
	private static RpcClient rpcClient = null;

	public static synchronized RpcClient getInstance()
			throws UnknownHostException {
		if (null == rpcClient) {
			rpcClient = new RpcClient();
		}
		return rpcClient;
	}

	private final RPCInterface iface;
	private final Client client;

	static interface RPCInterface {
		SplitTitleResponse spliteTitle(SplitTitleRequest request);

		ClusteringInfoResponse getClusteringInfos(ClusteringInfoRequest req);
	}

	public RpcClient() throws UnknownHostException {
		Configuration conf = Configuration.getInstance();
		EventLoop loop = EventLoop.defaultEventLoop();
		client = new Client(conf.getMsgpackAddress().split(",")[0],
				conf.getMsgpackPort(), loop);
		client.setRequestTimeout(1);
		iface = client.proxy(RPCInterface.class);
	}

	public void close() {
		client.close();
	}

	public SplitTitleResponse spliteTitle(SplitTitleRequest request) {
		return iface.spliteTitle(request);
	}

	public ClusteringInfoResponse getClusterInfos(ClusteringInfoRequest req) {
		return iface.getClusteringInfos(req);
	}
}
