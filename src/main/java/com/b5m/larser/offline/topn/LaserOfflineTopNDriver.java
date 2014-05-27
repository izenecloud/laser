package com.b5m.larser.offline.topn;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.msgpack.MessagePack;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.type.Value;

import com.b5m.couchbase.CouchbaseConfig;
import com.b5m.couchbase.CouchbaseInputFormat;
import com.b5m.larser.feature.UserProfileMap;
import com.b5m.msgpack.ClusteringInfo;
import com.b5m.msgpack.ClusteringInfoRequest;
import com.b5m.msgpack.ClusteringInfoResponse;
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
						.getMsgpackAddress(collection).split(",")[0],
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

	public static int serializeClusteringInfo(Path path, String address,
			Integer port, Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			return 0;
		}

		EventLoop loop = EventLoop.defaultEventLoop();
		Client client = null;
		try {
			client = new Client(address, port, loop);
			client.setRequestTimeout(10000);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		Object[] args = new Object[1];
		args[0] = new ClusteringInfoRequest();

		MessagePack msgpack = new MessagePack();
		msgpack.register(ClusteringInfoResponse.class);

		Value res = client.callApply("getClusteringInfos", args);
		ClusteringInfoResponse response = new org.msgpack.unpacker.Converter(
				msgpack, res).read(ClusteringInfoResponse.class);

		DataOutputStream out = fs.create(path);
		response.write(out);
		out.close();
		return 0;
	}
}
