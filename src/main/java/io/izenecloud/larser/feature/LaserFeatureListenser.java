package io.izenecloud.larser.feature;

import io.izenecloud.flume.B5MEvent;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;

public class LaserFeatureListenser implements MessageListener {

	private List<LaserMessageConsumer> consumer = new LinkedList<LaserMessageConsumer>();
	final ExecutorService executor = Executors.newFixedThreadPool(2);

	public synchronized void recieveMessages(Message message) {
		final DatumReader<B5MEvent> reader = new SpecificDatumReader<B5MEvent>(
				B5MEvent.SCHEMA$);

		final B5MEvent b5mEvent = new B5MEvent();

		byte[] data = message.getData();

		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
		try {
			reader.read(b5mEvent, decoder);
			for (LaserMessageConsumer consumer : this.consumer) {
				consumer.write(b5mEvent);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Executor getExecutor() {
		return executor;
	}

	public void setLaserMessageConsumer(LaserMessageConsumer consumer) {
		this.consumer.add(consumer);
	}
}
