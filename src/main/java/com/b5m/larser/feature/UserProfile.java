package com.b5m.larser.feature;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.mahout.math.Vector;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class UserProfile {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	@JsonProperty
	private String uuid;

	@JsonProperty
	private String date;

	@JsonProperty
	private int period;

	@JsonProperty
	private Map<String, Double> page_categories;

	@JsonProperty
	private Map<String, Double> product_categories;

	@JsonProperty
	private Map<String, Double> product_price;

	@JsonProperty
	private Map<String, Double> product_source;

	static public UserProfile createUserProfile(String jsonValue)
			throws JsonParseException, JsonMappingException, IOException {
		return OBJECT_MAPPER.readValue(jsonValue, UserProfile.class);
	}

	public void setUserFeature(Vector userFeature,
			final UserProfileHelper helper, Boolean add) {
		Set<Map.Entry<String, Double>> entrySet = page_categories.entrySet();
		Iterator<Map.Entry<String, Double>> iterator = entrySet.iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Double> entry = iterator.next();
			String key = "page_categories" + entry.getKey();

			Integer id = helper.map(key, add);
			if (null != id) {
				userFeature.set(id, entry.getValue());
			}
		}

		entrySet = product_categories.entrySet();
		iterator = entrySet.iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Double> entry = iterator.next();
			String key = "product_categories" + entry.getKey();
			Integer id = helper.map(key, add);
			if (null != id) {
				userFeature.set(id, entry.getValue());
			}
		}

		entrySet = product_price.entrySet();
		iterator = entrySet.iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Double> entry = iterator.next();
			String key = "product_price" + entry.getKey();
			Integer id = helper.map(key, add);
			if (null != id) {
				userFeature.set(id, entry.getValue());
			}
		}

		entrySet = product_source.entrySet();
		iterator = entrySet.iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Double> entry = iterator.next();
			String key = "product_source" + entry.getKey();
			Integer id = helper.map(key, add);
			if (null != id) {
				userFeature.set(id, entry.getValue());
			}
		}
	}
}
