package io.izenecloud.conf;

import io.izenecloud.larser.feature.LaserMessageConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

public class Configuration {

	private static Configuration conf = null;

	public static synchronized Configuration getInstance() {
		if (null == conf) {
			conf = new Configuration();
		}
		return conf;
	}

	public synchronized void load(Path path, FileSystem fs) throws IOException {
		final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
		FileStatus[] fileStatus = fs.listStatus(path, new GlobFilter(
				"*.properties"));
		for (FileStatus file : fileStatus) {
			if (file.isFile()) {
				Path p = file.getPath();
				FSDataInputStream in = fs.open(p);
				Collection configuration = OBJECT_MAPPER.readValue(in,
						Collection.class);
				String collection = p.getName().substring(0,
						p.getName().lastIndexOf(".properties"));
				configuration.setCollecion(collection);
				mapper.put(collection, configuration);
			}
		}
	}

	private Map<String, Collection> mapper = new HashMap<String, Collection>();

	public List<String> getCollections() {
		List<String> collectionList = new ArrayList<String>();
		Iterator<Map.Entry<String, Collection>> iterator = mapper.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			String collection = iterator.next().getKey();
			collectionList.add(collection);
		}
		return collectionList;
	}

	public void removeCollection(String collection) {
		mapper.remove(collection);
	}

	public Class<? extends LaserMessageConsumer> getMessageConsumer(
			String collection) throws ClassNotFoundException {
		return getCollection(collection).getMessageConsumer();
	}

	public Path getLaserHDFSRoot(String collection) {
		return getCollection(collection).getLaserHDFSRoot();

	}

	public String getCouchbaseCluster(String collection) {
		return getCollection(collection).getCouchbaseCluster();
	}

	public String getCouchbaseBucket(String collection) {
		return getCollection(collection).getCouchbaseBucket();

	}

	public String getCouchbasePassword(String collection) {
		return getCollection(collection).getCouchbasePassword();
	}

	public String getMetaqZookeeper(String collection) {
		return getCollection(collection).getMetaqZookeeper();
	}

	public String getMetaqTopic(String collection) {
		return getCollection(collection).getMetaqTopic();
	}

	public Path getMetaqOutput(String collection) {
		return getCollection(collection).getMetaqOutput();

	}

	public Path getLaserOnlineOutput(String collection) {
		return getCollection(collection).getLaserOnlineOutput();
	}

	public Path getLaserOfflineOutput(String collection) {
		return getCollection(collection).getLaserOfflineOutput();
	}

	public String getLaserOnlineRetrainingFreqency(String collection) {
		return getCollection(collection).getLaserOnlineRetrainingFreqency();
	}

	public String getLaserOfflineRetrainingFreqency(String collection) {
		return getCollection(collection).getLaserOfflineRetrainingFreqency();
	}

	public Integer getUserFeatureDimension(String collection) {
		return getCollection(collection).getUserFeatureDimension();
	}

	public Path getUserFeatureSerializePath(String collection) {
		return getCollection(collection).getUserFeatureSerializePath();
	}

	public Integer getItemFeatureDimension(String collection) {
		return getCollection(collection).getItemFeatureDimension();
	}

	public Integer getTopNClustering(String collection) {
		return getCollection(collection).getTopNClustering();
	}

	public Float getRegularizationFactor(String collection) {
		return getCollection(collection).getRegularizationFactor();
	}

	public Boolean addIntercept(String collection) {
		return getCollection(collection).addIntercept();
	}

	public Integer getMaxIteration(String collection) {
		return getCollection(collection).getMaxIteration();
	}

	public String getMsgpackAddress(String collection) {
		return getCollection(collection).getMsgpackAddress();
	}

	public Integer getMsgpackPort(String collection) {
		return getCollection(collection).getMsgpackPort();
	}

	private Collection getCollection(String collection) {
		return mapper.get(collection);
	}

	private String getProperty(Map<String, Map<String, String>> collection,
			String property, String propertyName) {
		return collection.get(property).get(propertyName);
	}
}
