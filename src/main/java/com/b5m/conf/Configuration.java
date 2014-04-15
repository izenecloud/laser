package com.b5m.conf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

public class Configuration {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String METAQ_FOLDER_NAME = "metaq_folder";
	private static final String OFFLINE_FOLDER_NAME = "offline_folder";
	private static final String ONLINE_MODEL_FOLDER = "online_model_folder";
	private static final String OFFLINE_MODEL_FOLDER = "offline_model_folder";

	@JsonProperty
	private Map<String, String> couchbase;

	@JsonProperty
	Map<String, String> metaq;

	@JsonProperty
	Map<String, String> msgpack;

	@JsonProperty
	Map<String, String> laser;

	private Path metaqFolder;
	private Path offlineFolder;
	private Path onlineModelFolder;
	private Path offlineModelFolder;

	private boolean isLoad;

	private static Configuration conf = null;

	public static synchronized Configuration getInstance() {
		if (null == conf) {
			conf = new Configuration();
		}
		return conf;
	}

	public synchronized void load(Path path, FileSystem fs) throws IOException {
		if (isLoad) {
			return;
		}
		FSDataInputStream in = fs.open(path);
		Configuration conf = OBJECT_MAPPER.readValue(in, Configuration.class);
		this.couchbase = conf.couchbase;
		this.metaq = conf.metaq;
		this.laser = conf.laser;
		this.msgpack = conf.msgpack;
		String baseOutput = laser.get("output");
		metaqFolder = new Path(baseOutput, METAQ_FOLDER_NAME);
		offlineFolder = new Path(baseOutput, OFFLINE_FOLDER_NAME);
		onlineModelFolder = new Path(baseOutput, ONLINE_MODEL_FOLDER);
		offlineModelFolder = new Path(baseOutput, OFFLINE_MODEL_FOLDER);
		isLoad = true;
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
		return metaqFolder;
	}

	public Path getLaserOnlineInput() {
		return metaqFolder;
	}

	public Path getLaserOnlineOutput() {
		return onlineModelFolder;
	}

	public Path getLaserOfflineInput() {
		return offlineFolder;
	}

	public Path getLaserOfflineOutput() {
		return offlineModelFolder;
	}

	public Long getLaserOnlineRetrainingFreqency() {
		return Long.valueOf(laser.get("online_retraining_frequency"));
	}

	public Long getLaserOfflineRetrainingFreqency() {
		return Long.valueOf(laser.get("offline_retraining_frequency"));
	}

	public Integer getUserFeatureDimension() {
		return Integer.valueOf(laser.get("user_feature_dimension"));
	}

	public Integer getItemFeatureDimension() {
		return Integer.valueOf(laser.get("item_feature_dimension"));
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

	public String getMsgpackAddress() {
		return msgpack.get("ip");
	}

	public Integer getMsgpackPort() {
		String port =  msgpack.get("port");
		if (null == port) {
			return null;
		}
		return Integer.valueOf(port);
	}
}
