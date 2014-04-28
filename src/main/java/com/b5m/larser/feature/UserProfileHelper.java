package com.b5m.larser.feature;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

final public class UserProfileHelper {
	private static UserProfileHelper helper = null;
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

	public String toString() {
		try {
			return OBJECT_MAPPER.writeValueAsString(userFeatureMap);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public synchronized Integer map(String key, Boolean add) {
		if (userFeatureMap.containsKey(key)) {
			return userFeatureMap.get(key);
		}
		if (add) {
			Integer val = userFeatureMap.size();
			userFeatureMap.put(key, val);
			return val;
		}
		return null;
	}

	public synchronized Integer size() {
		return userFeatureMap.size();
	}

	public void write(DataOutputStream out) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(out);
		synchronized (this) {
			oos.writeObject(userFeatureMap);
		}
		oos.close();
	}

	public static UserProfileHelper read(DataInputStream in)
			throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(in);
		helper = new UserProfileHelper();
		helper.userFeatureMap = (Map<String, Integer>) ois.readObject();
		return helper;
	}
}
