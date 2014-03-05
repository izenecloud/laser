package com.b5m.larser.source;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Component extends com.b5m.larser.dispatch.Component{
	private static final Logger LOG = LoggerFactory.getLogger(Component.class);
	private final String name;
	private final Map<String, Object> parameters;
	public Component(String name, Map<String, Object> parameters) {
		this.name = name;
		this.parameters = parameters;
	}

	@Override
	public void run() throws IOException{
		Path output = getComponentOutputPath(name);
		String inString = parameters.get("input").toString();
		if (null == inString) {
			LOG.error("<input> parameter is needed, which is empty");
			throw new IOException("<input> parameter is needed, which is empty");
		}
		Path input = new Path(inString);
		Configuration conf = new Configuration();
		FileSystem fs = input.getFileSystem(conf);
		
		if (!fs.exists(input)) {
			String errMsg = new String("Input: " + input + " does not exist");
			LOG.error(errMsg);
			throw new IOException(errMsg);
		}
		
		fs.copyToLocalFile(input, output);
	}

}
