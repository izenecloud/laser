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

		ClusterInfoResponse getClusteringInfos(ClusterInfoRequest req);

		void updateTopNCluster(PriorityQueue queue);

	}

	public RpcClient() throws UnknownHostException {
		Configuration conf = Configuration.getInstance();
		EventLoop loop = EventLoop.defaultEventLoop();
		client = new Client(conf.getMsgpackAddress(), conf.getMsgpackPort(),
				loop);
		client.setRequestTimeout(100000);

		iface = client.proxy(RPCInterface.class);
	}

	public void close() {
		client.close();
	}

	public SplitTitleResponse spliteTitle(SplitTitleRequest request) {
		return iface.spliteTitle(request);
	}

	public ClusterInfoResponse getClusterInfos(ClusterInfoRequest req) {
		return iface.getClusteringInfos(req);
	}

	public void updateTopNCluster(PriorityQueue queue) {
		iface.updateTopNCluster(queue);
	}

}
