package com.b5m.larser.dispatch;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class ComponentContext {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private String name;
	private String className;
	private Map<String, Object> parameters;

	@SuppressWarnings("unchecked")
	public ComponentContext(String name, String className,
			Map<String, Object> parameters) {
		this.name = name;
		this.className = className;
		this.parameters = parameters;
	}

	public String toJson() throws JsonGenerationException,
			JsonMappingException, IOException {
		Map<String, Object> map = new HashedMap();
		map.put("name", name);
		map.put("class", className);
		map.put("parameters", parameters);
		return OBJECT_MAPPER.writeValueAsString(map);
	}

	public Component newInstance() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		return ((Class<Component>) Class.forName(className)).getConstructor(String.class, Map.class).newInstance(
				name, parameters);
	}
}
