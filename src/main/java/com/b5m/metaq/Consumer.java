package com.b5m.metaq;

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

	private final MessageConsumer consumer;

	private Consumer() throws MetaClientException {
		final MetaClientConfig metaClientConfig = new MetaClientConfig();
		final ZKConfig zkConfig = new ZKConfig();
		zkConfig.zkConnect = com.b5m.metaq.ConsumerConfig.zookeeper;
		metaClientConfig.setZkConfig(zkConfig);
		MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(
				metaClientConfig);
		final String group = "meta-laser";
		consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
	}

	public void subscribe(String topic, MessageListener listener)
			throws MetaClientException {
		consumer.subscribe(topic, 1024 * 1024, listener);
		consumer.completeSubscribe();
	}

	public static void main(String[] args) throws Exception {
	}
}
