package com.b5m.larser.feature;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

final class UserProfileHelper {
	private static UserProfileHelper helper = null;

	public static synchronized UserProfileHelper getInstance() {
		if (null == helper) {
			helper = new UserProfileHelper();
		}
		return helper;
	}

	private Map<String, Integer> userFeatureMap;

	private UserProfileHelper() {
		userFeatureMap = new HashMap<String, Integer>();
	}

	public Integer map(String key) {
		if (userFeatureMap.containsKey(key)) {
			return userFeatureMap.get(key);
		}
		Integer val = userFeatureMap.size();
		userFeatureMap.put(key, val);
		return val;
	}

	public void write(DataOutputStream out) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeObject(userFeatureMap);
		oos.close();
	}

	public static UserProfileHelper read(DataInputStream in)
			throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(in);
		UserProfileHelper helper = new UserProfileHelper();
		helper.userFeatureMap = (Map<String, Integer>) ois.readObject();
		return helper;
	}
}
