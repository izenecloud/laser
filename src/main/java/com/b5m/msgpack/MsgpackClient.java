package com.b5m.msgpack;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.type.Value;

public class MsgpackClient {
	private final List<Client> clients;
	private final String collection;

	public MsgpackClient(String urlList, Integer port, String collection) {
		this.collection = collection;
		
		EventLoop loop = EventLoop.defaultEventLoop();
		clients = new LinkedList<Client>();
		try {
			for (String url : urlList.split(",")) {
				clients.add(new Client(url, port, loop));
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		for (Client client : clients) {
			try {
				client.close();
			} catch (Exception e) {
			}
		}
	}

	public void setTimeout(int requestTimeout) {
		for (Client client : clients) {
			client.setRequestTimeout(requestTimeout);
		}
	}
	public Value read(Object[] req, String method) {
		Object[] args = new Object[req.length + 1];
		args[0] = collection;
		for (int i = 1; i < args.length; i++) {
			args[i] = req[i-1];
		}
		for (Client client : clients) {
			try {
				return client.callApply(method, args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public Value write(Object[] req, String method) {
		Object[] args = new Object[req.length + 1];
		args[0] = collection;
		for (int i = 1; i < args.length; i++) {
			args[i] = req[i-1];
		}
		Value ret = null;
		args[0] = req;
		for (Client client : clients) {
			try {
				ret = client.callApply(method, args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}
}
