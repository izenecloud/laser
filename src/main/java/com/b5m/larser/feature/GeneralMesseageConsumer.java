package com.b5m.larser.feature;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.msgpack.MessagePack;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.flume.B5MEvent;
import com.b5m.msgpack.MsgpackClient;
import com.b5m.msgpack.SparseVector;
import com.couchbase.client.CouchbaseClient;

public class GeneralMesseageConsumer extends LaserMessageConsumer {
	private static final Logger LOG = LoggerFactory
			.getLogger(GeneralMesseageConsumer.class);

	private final Utf8 ACTION_ID_LABEL = new Utf8("ad");
	private final Utf8 TT_LABEL = new Utf8("tt");
	private final Utf8 TI_LABEL = new Utf8("ti");
	private final Utf8 UUID_LABEL = new Utf8("uid");

	private final int itemDimension;
	private final int userDimension;
	private final Path userFeatureMapper;
	private UserProfileMap mapper;
	private final FileSystem fs;

	private final CouchbaseClient couchbaseClient;
	private final MsgpackClient msgpackClient;

	public GeneralMesseageConsumer(String collection, Path output,
			FileSystem fs, Configuration conf) throws IOException {
		super(collection, output, fs, conf);
		this.fs = fs;
		this.userFeatureMapper = com.b5m.conf.Configuration.getInstance()
				.getUserFeatureSerializePath(getCollection());
		this.itemDimension = com.b5m.conf.Configuration.getInstance()
				.getItemFeatureDimension(getCollection());
		this.userDimension = com.b5m.conf.Configuration.getInstance()
				.getUserFeatureDimension(getCollection());

		if (fs.exists(this.userFeatureMapper)) {
			DataInputStream in = fs.open(this.userFeatureMapper);
			try {
				this.mapper = UserProfileMap.read(in);
				LOG.debug("user feature dimension: = {}", this.mapper.size());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				this.mapper = new UserProfileMap();
			}
			in.close();
		} else {
			this.mapper = new UserProfileMap();
		}

		List<URI> hosts = new ArrayList<URI>();

		try {
			List<String> uris = Arrays.asList(com.b5m.conf.Configuration
					.getInstance().getCouchbaseCluster(getCollection())
					.split(","));
			for (String uri : uris) {
				final URI ClusterURI = new URI(uri);
				hosts.add(ClusterURI.resolve("/pools"));
			}
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}

		couchbaseClient = new CouchbaseClient(hosts, com.b5m.conf.Configuration
				.getInstance().getCouchbaseBucket(getCollection()),
				com.b5m.conf.Configuration.getInstance().getCouchbasePassword(
						getCollection()));

		msgpackClient = new MsgpackClient(com.b5m.conf.Configuration
				.getInstance().getMsgpackAddress(collection),
				com.b5m.conf.Configuration.getInstance().getMsgpackPort(
						collection), collection);
	}

	public synchronized void shutdown() throws IOException {
		super.shutdown();
		couchbaseClient.shutdown();
		msgpackClient.close();
	}

	@Override
	public boolean write(B5MEvent b5mEvent) throws IOException {
		Map<CharSequence, CharSequence> args = b5mEvent.getArgs();
		CharSequence actionId = args.get(ACTION_ID_LABEL);
		if (null == actionId) {
			return false;
		}

		CharSequence uuid = args.get(UUID_LABEL);
		if (null == uuid) {
			return false;
		}
		String user = uuid.toString();

		String item = null;
		CharSequence title = args.get(TT_LABEL);
		if (null == title) {
			title = args.get(TI_LABEL);
			if (null == title) {
				return false;
			}
		}
		item = title.toString();

		Integer action = 1;
		if (actionId.toString().startsWith("103")) {
			action = 1;
		} else {
			action = -1;
		}

		Vector userFeature = new SequentialAccessSparseVector(userDimension);
		setUserProfile(user, userFeature);
		Vector itemFeature = new SequentialAccessSparseVector(itemDimension);
		if (!setItemProfile(item, itemFeature)) {
			// bad items
			return false;
		}

		append(new Text(user), new RequestWritable(userFeature, itemFeature,
				action));
		return true;
	}

	private boolean setUserProfile(String uuid, Vector profile)
			throws JsonParseException, JsonMappingException, IOException {
		try {
			Object res = couchbaseClient.get(uuid);
			if (null == res) {
				return false;
			}

			String jsonValue = res.toString();
			UserProfile userProfile = UserProfile.createUserProfile(jsonValue);
			userProfile.setUserFeature(profile, mapper, true);
		} catch (RuntimeException e) {
			return false;
		}
		return true;
	}

	private boolean setItemProfile(String title, Vector profile) {
		try {
			Object[] req = new Object[1];
			req[0] = title;
			Value res = msgpackClient.read(req, "spliteTitle");
			Converter converter = new org.msgpack.unpacker.Converter(res);
			SparseVector vec = converter.read(SparseVector.class);
			converter.close();
			
			while (vec.hasNext()) {
				profile.set(vec.getIndex(), vec.get());
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public void flush() throws IOException {
		DataOutputStream out = fs.create(userFeatureMapper);
		mapper.write(out);
		out.close();
	}
}
