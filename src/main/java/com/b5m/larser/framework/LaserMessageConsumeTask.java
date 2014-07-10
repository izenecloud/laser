package com.b5m.larser.framework;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.b5m.larser.feature.LaserFeatureListenser;
import com.b5m.larser.feature.LaserMessageConsumer;
import com.b5m.metaq.Consumer;
import com.taobao.metamorphosis.exception.MetaClientException;

public class LaserMessageConsumeTask {

	private LaserFeatureListenser listener = new LaserFeatureListenser();
	private Consumer consumer = new Consumer();
	private Map<String, LaserMessageConsumer> consumeTask = new HashMap<String, LaserMessageConsumer>();

	public LaserMessageConsumer getLaserMessageConsumer(String collection) {
		return consumeTask.get(collection);
	}

	public void addTask(String collection, LaserMessageConsumer task)
			throws MetaClientException {
		consumeTask.put(collection, task);
		listener.setLaserMessageConsumer(task);
		consumer.subscribe(collection, listener);
	}

	public void start() throws MetaClientException {
		consumer.completeSubscribe();
	}

	public void stop() {
		consumer.shutdown();
		Iterator<Map.Entry<String, LaserMessageConsumer>> iterator = consumeTask.entrySet().iterator();
		while (iterator.hasNext()) {
			try {
				iterator.next().getValue().shutdown();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
