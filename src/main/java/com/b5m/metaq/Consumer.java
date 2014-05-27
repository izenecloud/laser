package com.b5m.metaq;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.b5m.conf.Configuration;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class Consumer {

	private static Consumer metaqConsumer = null;

	public static synchronized Consumer getInstance()
			throws MetaClientException {
		if (null == metaqConsumer) {
			metaqConsumer = new Consumer();
		}
		return metaqConsumer;
	}

	private final Map<String, MessageConsumer> consumer;

	public Consumer() {
		consumer = new HashMap<String, MessageConsumer>();
	}

	public void subscribe(String collection, MessageListener listener)
			throws MetaClientException {
		final String urls = Configuration.getInstance().getMetaqZookeeper(
				collection);
		final String topic = Configuration.getInstance().getMetaqTopic(
				collection);

		if (this.consumer.containsKey(urls + topic)) {
			try {
				this.consumer.get(urls + topic).subscribe(topic, 1024 * 1024, listener);
			} catch (Exception e) {
				// ignore for
			}
		} else {

			final MetaClientConfig metaClientConfig = new MetaClientConfig();
			final ZKConfig zkConfig = new ZKConfig();
			zkConfig.zkConnect = urls;
			zkConfig.zkRoot = "/meta";

			metaClientConfig.setZkConfig(zkConfig);
			MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(
					metaClientConfig);
			final String group = "metaq-tare";
			ConsumerConfig consumerConfig = new ConsumerConfig(group);
			consumerConfig.setMaxDelayFetchTimeInMills(1000);
			MessageConsumer consumer = sessionFactory
					.createConsumer(consumerConfig);

			consumer.subscribe(topic, 1024 * 1024, listener);
			this.consumer.put(urls + topic, consumer);
		}
	}

	public void completeSubscribe() throws MetaClientException {
		Iterator<Entry<String, MessageConsumer>> iterator = this.consumer
				.entrySet().iterator();
		while (iterator.hasNext()) {
			MessageConsumer consumer = iterator.next().getValue();
			consumer.completeSubscribe();
		}
	}
	
	public void unsubscribe(String collection) throws MetaClientException {
		final String urls = Configuration.getInstance().getMetaqZookeeper(
				collection);
		final String topic = Configuration.getInstance().getMetaqTopic(
				collection);
		if (this.consumer.containsKey(urls + topic)) {
			MessageConsumer consumer = this.consumer.get(urls + topic);
			consumer.shutdown();
			this.consumer.remove(urls + topic);
		}
	}

	public void shutdown() {
		try {
			Iterator<Entry<String, MessageConsumer>> iterator = this.consumer
					.entrySet().iterator();
			while (iterator.hasNext()) {
				MessageConsumer consumer = iterator.next().getValue();
				consumer.shutdown();
			}
		} catch (MetaClientException e) {
			e.printStackTrace();
		}
	}
}
