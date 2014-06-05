package com.b5m.larser.offline.topn;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.msgpack.MessagePack;
import org.msgpack.type.Value;

import com.b5m.couchbase.CouchbaseConfig;
import com.b5m.couchbase.CouchbaseInputFormat;
import com.b5m.larser.feature.UserProfileMap;
import com.b5m.msgpack.AdClusteringsInfo;
import com.b5m.msgpack.MsgpackClient;
import com.b5m.msgpack.MsgpackOutputFormat;

public class LaserOfflineTopNDriver {
	public static int run(String collection, Integer topN,
			Configuration baseConf) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration(baseConf);

		conf.set(CouchbaseConfig.CB_INPUT_CLUSTER, com.b5m.conf.Configuration
				.getInstance().getCouchbaseCluster(collection));
		conf.set(CouchbaseConfig.CB_INPUT_BUCKET, com.b5m.conf.Configuration
				.getInstance().getCouchbaseBucket(collection));
		conf.set(CouchbaseConfig.CB_INPUT_PASSWORD, com.b5m.conf.Configuration
				.getInstance().getCouchbasePassword(collection));
		conf.setInt("laser.offline.topn.n", topN);
		conf.set("com.b5m.msgpack.ip", com.b5m.conf.Configuration.getInstance()
				.getMsgpackAddress(collection));

		conf.setInt("com.b5m.msgpack.port", com.b5m.conf.Configuration
				.getInstance().getMsgpackPort(collection));
		conf.set("com.b5m.msgpack.method", "updateTopNCluster");
		conf.set(
				"com.b5m.laser.offline.topn.offline.model",
				com.b5m.conf.Configuration.getInstance()
						.getLaserOfflineOutput(collection).toString());

		Path serializePath = com.b5m.conf.Configuration.getInstance()
				.getUserFeatureSerializePath(collection);
		UserProfileMap helper = UserProfileMap.getInstance();
		if (helper.size() == 0) {
			helper = null;
		} else {
			FileSystem fs = serializePath.getFileSystem(conf);
			DataOutputStream out = fs.create(serializePath);
			helper.write(out);
			out.close();
		}

		Path clusteringInfoPath = new Path(com.b5m.conf.Configuration
				.getInstance().getLaserHDFSRoot(collection), "clustering-info");

		serializeClusteringInfo(
				clusteringInfoPath,
				com.b5m.conf.Configuration.getInstance()
						.getMsgpackAddress(collection),
				com.b5m.conf.Configuration.getInstance().getMsgpackPort(
						collection), conf);

		conf.set("com.b5m.laser.offline.topn.clustering.info",
				clusteringInfoPath.toString());
		conf.set("com.b5m.laser.offline.topn.user.feature.map",
				serializePath.toString());

		Job job = Job.getInstance(conf);
		job.setJarByClass(LaserOfflineTopNDriver.class);
		job.setJobName("calculate top n clusters for each user");
		// TODO addCachFile
		// job.addCacheFile(serializePath.toUri());
		// job.addCacheFile(new Path(com.b5m.conf.Configuration.getInstance()
		// .getLaserOfflineOutput(), "A").toUri());
		// job.addCacheFile(new Path(com.b5m.conf.Configuration.getInstance()
		// .getLaserOfflineOutput(), "alpha").toUri());
		// job.addCacheFile(new Path(com.b5m.conf.Configuration.getInstance()
		// .getLaserOfflineOutput(), "beta").toUri());

		job.setInputFormatClass(CouchbaseInputFormat.class);
		job.setOutputFormatClass(MsgpackOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PriorityQueue.class);

		job.setMapperClass(LaserOfflineTopNMapper.class);
		job.setNumReduceTasks(0);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}

		clusteringInfoPath.getFileSystem(conf).delete(clusteringInfoPath);
		return 0;
	}

	public static int serializeClusteringInfo(Path path, String urlList,
			Integer port, Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			return 0;
		}
		
		MsgpackClient client = new MsgpackClient(urlList, port, conf.get("com.b5m.msgpack.collection"));
		client.setTimeout(1000);
		Value res = client.read(new Object[0], "getClusteringInfos");
		AdClusteringsInfo response = new org.msgpack.unpacker.Converter(
				new MessagePack(), res).read(AdClusteringsInfo.class);

		DataOutputStream out = fs.create(path);
		response.write(out);
		out.close();
		return 0;
	}
}
