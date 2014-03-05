package com.b5m.larser.transformer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Component extends com.b5m.larser.dispatch.Component{
	private static final Logger LOG = LoggerFactory.getLogger(Component.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final String name;
	private final Map<String, Object> parameters;
	public Component(String name, Map<String, Object> parameters) {
		this.name = name;
		this.parameters = parameters;
	}
	
	@Override
	public void run() throws IOException {
		String in = parameters.get("input").toString();
		List<String> features = (List<String>)parameters.get("features");
		Path input = getComponentOutputPath(in);
		Configuration conf = new Configuration();
		FileSystem fs = input.getFileSystem(conf);
		FSDataInputStream inputStream = fs.open(input);
		Path output = getComponentOutputPath(name);
		FSDataOutputStream  outputStream = fs.create(output);
		String content;
		while (null != (content = inputStream.readLine())) {
			Map<String, String> ivalue = OBJECT_MAPPER.readValue(content, Map.class);
			Map<String, String> ovalue = new HashedMap();
			Iterator<String> iterator = features.iterator();
			while (iterator.hasNext()) {
				String featureId = iterator.next();
				String feature = ivalue.get(featureId);
				if (null != feature) {
					ovalue.put(featureId, feature);
				}
			}
			outputStream.writeUTF(OBJECT_MAPPER.writeValueAsString(ovalue));
		}
		inputStream.close();
		outputStream.close();
	}
}