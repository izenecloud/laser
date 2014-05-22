package com.b5m.msgpack;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.msgpack.annotation.Message;

@Message
public class ClusteringInfoResponse {
	private List<ClusteringInfo> clusteringInfo;

	public Iterator<ClusteringInfo> iterator() {
		return clusteringInfo.iterator();
	}

	public int size() {
		return clusteringInfo.size();
	}

	public void write(DataOutputStream out) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(out);
		synchronized (this) {
			oos.writeInt(clusteringInfo.size());
			Iterator<ClusteringInfo> iterator = clusteringInfo.iterator();
			while (iterator.hasNext()) {
				ClusteringInfo info = iterator.next();
				oos.writeObject(info.clusteringIndex);
				oos.writeObject(info.pows);
			}
		}
		oos.close();
	}

	public static ClusteringInfoResponse read(DataInputStream in)
			throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(in);
		ClusteringInfoResponse clustering = new ClusteringInfoResponse();
		int size = ois.readInt();
		clustering.clusteringInfo = new ArrayList<ClusteringInfo>(size);
		for (int i = 0; i < size; i++) {
			ClusteringInfo info = new ClusteringInfo();
			info.clusteringIndex = (Integer) ois.readObject();
			info.pows = (Map<Integer, Float>) ois.readObject();
			clustering.clusteringInfo.add(info);
		}
		return clustering;
	}
}
