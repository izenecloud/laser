package io.izenecloud.larser.offline.topn;

import io.izenecloud.couchbase.CouchbaseConfig;
import io.izenecloud.couchbase.CouchbaseInputFormat;
import io.izenecloud.larser.feature.UserProfileMap;
import io.izenecloud.msgpack.AdClusteringsInfo;
import io.izenecloud.msgpack.MsgpackClient;
import io.izenecloud.msgpack.MsgpackOutputFormat;
import io.izenecloud.msgpack.SparseVector;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.msgpack.MessagePack;
import org.msgpack.type.Value;

public class LaserOfflineTopNDriver {
	public static int run(String collection, Integer topN,
			Configuration baseConf) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration(baseConf);

		conf.setInt("laser.offline.topn.n", topN);		
		conf.set("com.b5m.laser.msgpack.output.method", "update_topn_clustering");
		conf.set(
				"com.b5m.laser.offline.topn.offline.model",
				io.izenecloud.conf.Configuration.getInstance()
						.getLaserOfflineOutput(collection).toString());

		Path serializePath = io.izenecloud.conf.Configuration.getInstance()
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

		Path clusteringInfoPath = new Path(io.izenecloud.conf.Configuration
				.getInstance().getLaserHDFSRoot(collection), "clustering-info");

		serializeClusteringInfo(
				clusteringInfoPath,
				io.izenecloud.conf.Configuration.getInstance()
						.getMsgpackAddress(collection),
				io.izenecloud.conf.Configuration.getInstance().getMsgpackPort(
						collection), conf);

		conf.set("com.b5m.laser.offline.topn.clustering.info",
				clusteringInfoPath.toString());
		conf.set("com.b5m.laser.offline.topn.user.feature.map",
				serializePath.toString());

		Job job = Job.getInstance(conf);
		job.setJarByClass(LaserOfflineTopNDriver.class);
		job.setJobName("calculate top n clusters for each user");

		job.setInputFormatClass(CouchbaseInputFormat.class);
		job.setOutputFormatClass(MsgpackOutputFormat.class);

		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(SparseVector.class);

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
