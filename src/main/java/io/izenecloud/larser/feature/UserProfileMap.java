package io.izenecloud.larser.feature;

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

final public class UserProfileMap {
	private static UserProfileMap mapper = null;
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static synchronized UserProfileMap getInstance() {
		if (null == mapper) {
			mapper = new UserProfileMap();
		}
		return mapper;
	}

	private Map<String, Integer> userFeatureMap;

	public UserProfileMap() {
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

	public static UserProfileMap read(DataInputStream in) throws IOException,
			ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(in);
		mapper = new UserProfileMap();
		mapper.userFeatureMap = (Map<String, Integer>) ois.readObject();
		return mapper;
	}
}
