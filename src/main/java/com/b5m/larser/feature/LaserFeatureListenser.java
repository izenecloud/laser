package com.b5m.larser.feature;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.b5m.flume.B5MEvent;
import com.couchbase.client.CouchbaseClient;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;

public class LaserFeatureListenser implements MessageListener {

	private final Utf8 LOG_TYPE_LABEL = new Utf8("lt");
	private final Utf8 ACTION_ID_LABEL = new Utf8("ad");
	private final Utf8 ITEM_LABEL = new Utf8("tt");
	private final Utf8 UUID_LABEL = new Utf8("uid");

	private final DatumReader<B5MEvent> reader = new SpecificDatumReader<B5MEvent>(
			B5MEvent.SCHEMA$);

	@SuppressWarnings("deprecation")
	private final DecoderFactory decoderFactor = DecoderFactory
			.defaultFactory();
	private BinaryDecoder decorder = null;
	private final B5MEvent b5mEvent = new B5MEvent();

	private final int featureDimension;
	private final Path output;
	private final FileSystem fs;
	private final Configuration conf;
	private SequenceFile.Writer writer;
	private long sequentialNumber = 0;

	private final CouchbaseClient couchbaseClient;

	public LaserFeatureListenser(String url, String bucket, String passwd,
			Path output, FileSystem fs, Configuration conf, int featureDimension)
			throws IOException, URISyntaxException {
		this.output = output;
		this.fs = fs;
		this.conf = conf;
		this.featureDimension = featureDimension;
		initSequenceWriter();
		List<URI> hosts = Arrays.asList(new URI(url));
		couchbaseClient = new CouchbaseClient(hosts, bucket, passwd);
	}

	public void shutdown() {
		couchbaseClient.shutdown();
	}

	@SuppressWarnings("deprecation")
	public void incrSequentialNumber() throws IOException {
		synchronized (this) {
			writer.close();
		}
		sequentialNumber++;
		synchronized (this) {
			writer = SequenceFile.createWriter(fs, conf,
					new Path(output, Long.toString(sequentialNumber)),
					IntWritable.class, VectorWritable.class);
		}
	}

	@SuppressWarnings("deprecation")
	private void initSequenceWriter() throws IOException {
		Path sequentialPath = new Path(output, Long.toString(sequentialNumber));
		while (fs.exists(sequentialPath)) {
			sequentialNumber++;
			sequentialPath = new Path(output, Long.toString(sequentialNumber));
		}
		synchronized (this) {
			writer = SequenceFile.createWriter(fs, conf, sequentialPath,
					IntWritable.class, VectorWritable.class);
		}
	}

	public void recieveMessages(Message message) {
		byte[] data = message.getData();
		decorder = decoderFactor.binaryDecoder(data, decorder);
		try {
			reader.read(b5mEvent, decorder);
			write(b5mEvent);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Executor getExecutor() {
		// TODO Auto-generated method stub
		return null;
	}

	private void write(B5MEvent b5mEvent) throws IOException {
		Map<CharSequence, CharSequence> args = b5mEvent.getArgs();
		// TODO filter by logtype and action
		CharSequence logType = args.get(LOG_TYPE_LABEL);
		if (null == logType) {
			return;
		}
		CharSequence actionId = args.get(ACTION_ID_LABEL);
		if (null == actionId) {
			return;
		}

		CharSequence uuid = args.get(UUID_LABEL);
		if (null == uuid) {
			return;
		}
		String user = uuid.toString();

		CharSequence title = args.get(ITEM_LABEL);
		if (null == title) {
			return;
		}
		String item = title.toString();
		Vector feature = new SequentialAccessSparseVector(featureDimension + 1);
		setUserFeature(user, feature);
		setItemFeature(item, feature);
		writer.append(new IntWritable(user.hashCode()), new VectorWritable(
				feature));
	}

	private void setUserFeature(String user, Vector feature)
			throws JsonParseException, JsonMappingException, IOException {
		String jsonValue = couchbaseClient.get(user).toString();
		UserProfile userProfile = UserProfile.createUserProfile(jsonValue);
		userProfile.setUserFeature(feature);
	}

	private void setItemFeature(String item, Vector feature) {

	}
}
