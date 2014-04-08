package com.b5m.msgpack;

import java.net.UnknownHostException;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;

import com.b5m.conf.Configuration;
import com.b5m.larser.feature.SplitTitleRequest;
import com.b5m.larser.feature.SplitTitleResponse;
import com.b5m.larser.offline.LaserOfflineModel;
import com.b5m.larser.online.LaserOnlineModel;

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

		void updateLaserOfflineModel(LaserOfflineModel model);

		void updateLaserOnlineModel(LaserOnlineModel model);
	}

	public RpcClient() throws UnknownHostException {
		Configuration conf = Configuration.getInstance();
		EventLoop loop = EventLoop.defaultEventLoop();
		client = new Client(conf.getMsgpackAddress(), conf.getMsgpackPort(),
				loop);
		client.setRequestTimeout(1);

		iface = client.proxy(RPCInterface.class);
	}

	public void close() {
		client.close();
	}

	public void updateLaserOfflineModel(LaserOfflineModel model) {
		iface.updateLaserOfflineModel(model);
	}

	public void updateLaserOnlineModel(LaserOnlineModel model) {
		iface.updateLaserOnlineModel(model);
	}
	
	public SplitTitleResponse spliteTitle(SplitTitleRequest request) {
		return iface.spliteTitle(request);
	}
}
