package io.izenecloud.msgpack;

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
public class AdClusteringsInfo {
	private List<SparseVector> infos;

	public Iterator<SparseVector> iterator() {
		return infos.iterator();
	}

	public int size() {
		return infos.size();
	}

	public void write(DataOutputStream out) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(out);
		synchronized (this) {
			oos.writeInt(infos.size());
			Iterator<SparseVector> iterator = infos.iterator();
			while (iterator.hasNext()) {
				SparseVector sv = iterator.next();
				oos.writeObject(sv.index);
				oos.writeObject(sv.value);
			}
		}
		oos.close();
	}

	public static AdClusteringsInfo read(DataInputStream in)
			throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(in);
		AdClusteringsInfo clustering = new AdClusteringsInfo();
		int size = ois.readInt();
		clustering.infos = new ArrayList<SparseVector>(size);
		for (int i = 0; i < size; i++) {
			SparseVector sv = new SparseVector();
			sv.index = (List<Integer>) ois.readObject();
			sv.value = (List<Float>) ois.readObject();
		}
		return clustering;
	}
}
