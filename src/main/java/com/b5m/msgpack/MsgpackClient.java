package com.b5m.msgpack;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.type.Value;

public class MsgpackClient {
	private final List<Client> clients;

	public MsgpackClient(String urlList, Integer port) {
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

	public Value read(Object req, String method) {
		Object[] args = new Object[1];
		args[0] = req;
		for (Client client : clients) {
			try {
				return client.callApply(method, args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public Value write(Object req, String method) {
		Value ret = null;
		Object[] args = new Object[1];
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
