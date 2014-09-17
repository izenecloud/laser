package io.izenecloud.conf;

import io.izenecloud.larser.feature.LaserMessageConsumer;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonProperty;

class Collection {
	private static final String METAQ_FOLDER_NAME = "metaq_folder";
	private static final String ONLINE_MODEL_FOLDER = "online_model_folder";
	private static final String OFFLINE_MODEL_FOLDER = "offline_model_folder";

	@JsonProperty
	private Map<String, String> couchbase;

	@JsonProperty
	private Map<String, String> metaq;

	@JsonProperty
	private Map<String, String> msgpack;

	@JsonProperty
	private Map<String, String> laser;

	private String collection;

	public void setCollecion(String collection) {
		this.collection = collection;
	}

	public Path getLaserHDFSRoot() {
		return new Path(laser.get("output"), collection);
	}

	public String getCouchbaseCluster() {
		return couchbase.get("cluster");
	}

	public String getCouchbaseBucket() {
		return couchbase.get("bucket");
	}

	public String getCouchbasePassword() {
		return couchbase.get("passwd");
	}

	public String getMetaqZookeeper() {
		return metaq.get("zookeeper");
	}

	public String getMetaqTopic() {
		return metaq.get("topic");
	}

	public Path getMetaqOutput() {
		return new Path(getLaserHDFSRoot(), METAQ_FOLDER_NAME);
	}

	public Path getLaserOnlineOutput() {
		return new Path(getLaserHDFSRoot(), ONLINE_MODEL_FOLDER);
	}

	public Path getLaserOfflineOutput() {
		return new Path(getLaserHDFSRoot(), OFFLINE_MODEL_FOLDER);
	}

	public String getLaserOnlineRetrainingFreqency() {
		return laser.get("online_retraining_frequency");
	}

	public String getLaserOfflineRetrainingFreqency() {
		return laser.get("offline_retraining_frequency");
	}

	public Integer getUserFeatureDimension() {
		return Integer.valueOf(laser.get("user_feature_dimension"));
	}

	public Path getUserFeatureSerializePath() {
		return new Path(getMetaqOutput(), "USER_FEATURE_MAP");
	}

	public Integer getItemFeatureDimension() {
		return Integer.valueOf(laser.get("item_feature_dimension"));
	}

	public Integer getTopNClustering() {
		return Integer.valueOf(laser.get("top_n_clustering"));
	}

	public Float getRegularizationFactor() {
		String regularization_factor = laser.get("regularization_factor");
		if (null == regularization_factor) {
			return null;
		}
		return Float.valueOf(regularization_factor);
	}

	public Boolean addIntercept() {
		String add_intercept = laser.get("add_intercept");
		if (null == add_intercept) {
			return null;
		}
		return Boolean.valueOf(add_intercept);
	}

	public Integer getMaxIteration() {
		String offline_max_iteration = laser.get("offline_max_iteration");
		if (null == offline_max_iteration) {
			return null;
		}
		return Integer.valueOf(offline_max_iteration);
	}

	public Class<? extends LaserMessageConsumer> getMessageConsumer()
			throws ClassNotFoundException {
		return (Class<? extends LaserMessageConsumer>) Class.forName(metaq
				.get("consumer"));
	}

	public String getMsgpackAddress() {
		return msgpack.get("ip");
	}

	public Integer getMsgpackPort() {
		String port = msgpack.get("port");
		if (null == port) {
			return null;
		}
		return Integer.valueOf(port);
	}
}
