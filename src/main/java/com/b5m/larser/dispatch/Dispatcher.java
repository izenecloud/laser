package com.b5m.larser.dispatch;

import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dispatcher {
	private static final PropertiesLoader LOADER = new PropertiesLoader();
	private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);
	
	public boolean validate() {
		// TODO
		return true;
	}
	
	public void run(Path file, FileSystem fs) throws ClassNotFoundException, IOException {
		LOADER.load(file, fs);
		if (!validate()) {
			String errorMsg = new String("Fatal error in " + file);
			LOG.error(errorMsg);
			throw new IOException(errorMsg);
		}
		Iterator<ComponentContext> iterator = LOADER.componentContexts().iterator();
		while (iterator.hasNext()) {
			ComponentContext comContext = iterator.next();
			try {
				LOG.debug("running Component {}", comContext.toJson());
				comContext.newInstance().run();
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("Component {} is failed for {}", comContext.toJson(), e.getMessage());
				throw new IOError(e.getCause());
			} 
		}
	}
}
