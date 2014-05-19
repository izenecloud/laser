package com.b5m.larser.feature;

import java.io.DataInputStream;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.flume.B5MEvent;
import com.couchbase.client.CouchbaseClient;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;

import edu.stanford.nlp.io.EncodingPrintWriter.out;

public class LaserFeatureListenser implements MessageListener {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserFeatureListenser.class);
	private final Utf8 LOG_TYPE_LABEL = new Utf8("lt");
	private final Utf8 ACTION_ID_LABEL = new Utf8("ad");
	private final Utf8 ITEM_LABEL = new Utf8("tt");
	private final Utf8 UUID_LABEL = new Utf8("uid");

	private final DatumReader<B5MEvent> reader = new SpecificDatumReader<B5MEvent>(
			B5MEvent.SCHEMA$);

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
	private UserProfileHelper helper;

	public LaserFeatureListenser(String url, String bucket, String passwd,
			Path output, FileSystem fs, Configuration conf, int itemDimension,
			int userDimension) throws IOException, URISyntaxException {

		this.output = output;
		this.fs = fs;
		this.conf = conf;
		this.itemDimension = itemDimension;
		this.userDimension = userDimension;

		Path serializePath = com.b5m.conf.Configuration.getInstance()
				.getUserFeatureSerializePath();
		if (fs.exists(serializePath)) {
			DataInputStream in = fs.open(serializePath);
			try {
				this.helper = UserProfileHelper.read(in);
				LOG.debug("user feature dimension: = {}", this.helper.size());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				this.helper = UserProfileHelper.getInstance();
			}
			in.close();
		} else {
			this.helper = UserProfileHelper.getInstance();
		}

		initSequenceWriter();
		List<URI> hosts = Arrays.asList(new URI(url));
		couchbaseClient = new CouchbaseClient(hosts, bucket, passwd);
	}

	public synchronized void shutdown() {
		couchbaseClient.shutdown();
	}

	@SuppressWarnings("deprecation")
	public synchronized void incrMinorVersion() throws IOException {
		writer.close();

		minorVersion++;
		writer = SequenceFile.createWriter(
				fs,
				conf,
				new Path(output, Long.toString(majorVersion) + "-"
						+ Long.toString(minorVersion)), Text.class,
				RequestWritable.class);

	}

	public synchronized long geMinorVersion() {
		return minorVersion;
	}

	@SuppressWarnings("deprecation")
	public synchronized void incrMajorVersion() throws IOException {
		writer.close();

		majorVersion++;
		minorVersion = 0;
		writer = SequenceFile.createWriter(
				fs,
				conf,
				new Path(output, Long.toString(majorVersion) + "-"
						+ Long.toString(minorVersion)), Text.class,
				RequestWritable.class);

	}

	public synchronized long geMajorVersion() {
		return majorVersion;
	}

	@SuppressWarnings("deprecation")
	private synchronized void initSequenceWriter() throws IOException {

		try {
			FileStatus[] files = fs.listStatus(output);

			for (FileStatus file : files) {
				String name = file.getPath().getName();
				String[] versions = name.split("-");
				if (versions.length != 2) {
					continue;
				}
				Long majorVersion = Long.valueOf(versions[0]);
				if (this.majorVersion < majorVersion) {
					this.majorVersion = majorVersion;
				}
				Long minorVersion = Long.valueOf(versions[1]);
				if (this.minorVersion < minorVersion) {
					this.minorVersion = minorVersion;
				}
			}
		} catch (IOException e) {
			LOG.debug("{} doesn't exist", output);
		}

		minorVersion++;
		Path sequentialPath = new Path(output, Long.toString(majorVersion)
				+ "-" + Long.toString(minorVersion));

		LOG.debug("writing data to {}", sequentialPath);
		writer = SequenceFile.createWriter(fs, conf, sequentialPath,
				Text.class, RequestWritable.class);
	}

	public synchronized void recieveMessages(Message message) {
		byte[] data = message.getData();

		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
		try {
			reader.read(b5mEvent, decoder);
			write(b5mEvent);
		} catch (Exception e) {
			e.printStackTrace();
			// System.out.println(b5mEvent.toString());
			// System.out.println(b5mEvent.getArgs().toString());
		}
	}

	public Executor getExecutor() {
		return null;
	}

	private void write(B5MEvent b5mEvent) throws IOException {
		LOG.debug(b5mEvent.toString());
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

		String item = null;
		CharSequence title = args.get(ITEM_LABEL);
		if (null == title) {
			// item = "";
			return;
		} else {
			item = title.toString();
		}

		// if (2000 != Integer.valueOf(logType.toString())) {
		// // System.out.println(logType.toString());
		// return;
		// }
		Integer action = 1;
		if (108 == Integer.valueOf(actionId.toString())) {
			action = -1;
			// return;
		} else if (103 == Integer.valueOf(actionId.toString())) {
			action = 1;
		} else {
			return;
		}
		Vector userFeature = new SequentialAccessSparseVector(userDimension);
		setUserFeature(user, userFeature);
		Vector itemFeature = new SequentialAccessSparseVector(itemDimension);
		setItemFeature(item, itemFeature);

		// bad items
		if (itemFeature.norm(2) < 1e-6) {
			return;
		}
		writer.append(new Text(user), new RequestWritable(userFeature,
				itemFeature, action));
	}

	private void setUserFeature(String user, Vector feature)
			throws JsonParseException, JsonMappingException, IOException {
		try {
			Object res = couchbaseClient.get(user);
			if (null == res) {
				return;
			}

			String jsonValue = res.toString();
			UserProfile userProfile = UserProfile.createUserProfile(jsonValue);
			userProfile.setUserFeature(feature, helper, true);
		} catch (RuntimeException e) {
			// TODO timeout
			// System.out.println(user);
		}
	}

	private void setItemFeature(String item, Vector feature) {
		ItemProfile.setItemFeature(item, feature);
	}
}
