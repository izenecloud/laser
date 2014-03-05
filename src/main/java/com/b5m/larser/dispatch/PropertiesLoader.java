package com.b5m.larser.dispatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesLoader {
	private static final Logger LOG = LoggerFactory
			.getLogger(PropertiesLoader.class.getName());
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private List<ComponentContext> components;

	public PropertiesLoader() {
		components = new LinkedList<ComponentContext>();
	}

	public void load(Path file, FileSystem fs) throws IOException, ClassNotFoundException {
		if (!fs.exists(file)) {
			LOG.error("Properties file: {} does not exist.", file);
		}

		FSDataInputStream in = fs.open(file);
		JsonNode tree = OBJECT_MAPPER.readTree(in).get("objects");
		Iterator<JsonNode> iterator = tree.iterator();
		while (iterator.hasNext()) {
			JsonNode node = iterator.next();

			@SuppressWarnings("unchecked")
			ComponentContext com = new ComponentContext(node.get("name").getTextValue(), node
					.get("class").getTextValue(), OBJECT_MAPPER.readValue(node
					.get("parameters").toString(), Map.class));
			components.add(com);
			LOG.debug("new component: {}", com.toJson());
		}
	}
	
	public List<ComponentContext> componentContexts() {
		return components;
	}
}
