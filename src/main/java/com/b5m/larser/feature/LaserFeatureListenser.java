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

	private final int itemDimension;
	private final int userDimension;
	private final Path output;
	private final FileSystem fs;
	private final Configuration conf;
	private SequenceFile.Writer writer;
	private long majorVersion = 0;
	private long minorVersion = 0;

	private final CouchbaseClient couchbaseClient;
	private final ItemProfile itemProfileProvider;

	private boolean threadSuspended;

	public LaserFeatureListenser(String url, String bucket, String passwd,
			Path output, FileSystem fs, Configuration conf, int itemDimension,
			int userDimension) throws IOException, URISyntaxException {
		this.output = output;
		this.fs = fs;
		this.conf = conf;
		this.itemDimension = itemDimension;
		this.userDimension = userDimension;

		initSequenceWriter();
		List<URI> hosts = Arrays.asList(new URI(url));
		couchbaseClient = new CouchbaseClient(hosts, bucket, passwd);
		itemProfileProvider = new ItemProfile();
		threadSuspended = false;
	}

	public void shutdown() {
		couchbaseClient.shutdown();
		itemProfileProvider.close();
	}

	@SuppressWarnings("deprecation")
	public void incrMinorVersion() throws IOException {
		synchronized (this) {
			writer.close();
			threadSuspended = true;
		}
		minorVersion++;
		synchronized (this) {
			writer = SequenceFile.createWriter(
					fs,
					conf,
					new Path(output, Long.toString(majorVersion) + "-"
							+ Long.toString(minorVersion)), IntWritable.class,
					RequestWritable.class);
			threadSuspended = false;
		}
	}

	public synchronized long geMinorVersion() {
		return minorVersion;
	}

	@SuppressWarnings("deprecation")
	public void incrMajorVersion() throws IOException {
		synchronized (this) {
			writer.close();
			threadSuspended = true;
		}
		majorVersion++;
		synchronized (this) {
			writer = SequenceFile.createWriter(
					fs,
					conf,
					new Path(output, Long.toString(majorVersion) + "-"
							+ Long.toString(minorVersion)), IntWritable.class,
					RequestWritable.class);
			threadSuspended = false;
		}
	}

	public synchronized long geMajorVersion() {
		return majorVersion;
	}

	@SuppressWarnings("deprecation")
	private void initSequenceWriter() throws IOException {
		Path sequentialPath = new Path(output, Long.toString(majorVersion)
				+ "-" + Long.toString(minorVersion));
		while (fs.exists(sequentialPath)) {
			minorVersion++;
			sequentialPath = new Path(output, Long.toString(majorVersion) + "-"
					+ Long.toString(minorVersion));
		}
		synchronized (this) {
			writer = SequenceFile.createWriter(fs, conf, sequentialPath,
					IntWritable.class, RequestWritable.class);
		}
	}

	public void recieveMessages(Message message) {
		synchronized (this) {
			while (threadSuspended) {

			}
		}
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
		Integer action = 1;
		String item = title.toString();
		Vector userFeature = new SequentialAccessSparseVector(userDimension);
		setUserFeature(user, userFeature);
		// setItemFeature(item, feature);
		Vector itemFeature = new SequentialAccessSparseVector(itemDimension);
		setItemFeature(item, itemFeature);
		writer.append(new IntWritable(user.hashCode()), new RequestWritable(
				userFeature, itemFeature, action));
	}

	private void setUserFeature(String user, Vector feature)
			throws JsonParseException, JsonMappingException, IOException {
		String jsonValue = couchbaseClient.get(user).toString();
		UserProfile userProfile = UserProfile.createUserProfile(jsonValue);
		userProfile.setUserFeature(feature);
	}

	private void setItemFeature(String item, Vector feature) {
		itemProfileProvider.setItemFeature(item, feature);
	}
}
