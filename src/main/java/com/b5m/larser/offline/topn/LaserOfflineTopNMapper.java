package com.b5m.larser.offline.topn;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.larser.feature.UserProfile;
import com.b5m.msgpack.ClusterInfo;
import com.b5m.msgpack.ClusterInfoRequest;
import com.b5m.msgpack.ClusterInfoResponse;
import com.b5m.msgpack.RpcClient;

import static com.b5m.HDFSHelper.readMatrix;
import static com.b5m.HDFSHelper.readVector;

public class LaserOfflineTopNMapper
		extends
		Mapper<BytesWritable, BytesWritable, NullWritable, com.b5m.msgpack.PriorityQueue> {
	private Vector alpha;
	private List<Double> CBeta;
	private List<IntVector> AC;
	private Vector userFeature;

	private int TOP_N;
	PriorityQueue queue;

	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOfflineTopNMapper.class);

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String offlineModel = conf
				.get("com.b5m.laser.offline.topn.offline.model");
		Path offlinePath = new Path(offlineModel);
		FileSystem fs = offlinePath.getFileSystem(conf);
		this.alpha = readVector(new Path(offlinePath, "alpha"), fs, conf);
		Vector beta = readVector(new Path(offlinePath, "beta"), fs, conf);
		Matrix A = readMatrix(new Path(offlinePath, "A"), fs, conf);

		AC = new LinkedList<IntVector>();
		CBeta = new LinkedList<Double>();

		for (int i = 0; i < 2000; i++) {
			Vector acj = new DenseVector(A.numRows());
			AC.add(new IntVector(i, acj));
			CBeta.add(0.5);
		}

		// ClusterInfoRequest req = new ClusterInfoRequest();
		// ClusterInfoResponse response =
		// RpcClient.getInstance().getClusterInfos(
		// req);
		// Iterator<ClusterInfo> iterator = response.iterator();
		// while (iterator.hasNext()) {
		// ClusterInfo info = iterator.next();
		// com.b5m.larser.offline.topn.ClusterInfo cluster = new
		// com.b5m.larser.offline.topn.ClusterInfo(
		// info.clusterHash, info.pows);
		// // A * Cj
		// Vector acj = new DenseVector(A.numRows());
		// for (int row = 0; row < A.numRows(); row++) {
		// Vector ai = A.viewRow(row);
		// acj.set(row, cluster.getClusterFeatureVector().dot(ai));
		// }
		// AC.add(new IntVector(cluster.getClusterHash(), acj));
		//
		// // Cj * beta
		// CBeta.add(cluster.getClusterFeatureVector().dot(beta));
		// }

		userFeature = new SequentialAccessSparseVector(A.numRows());

		TOP_N = conf.getInt("laser.offline.topn.n", 5);
		queue = new PriorityQueue(TOP_N);
	}

	protected void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		UserProfile user = UserProfile
				.createUserProfile(new String(value.get()));
		user.setUserFeature(userFeature);

		Iterator<IntVector> acIterator = AC.iterator();
		Iterator<Double> cBetaIterator = CBeta.iterator();
		while (acIterator.hasNext()) {
			IntVector intVec = acIterator.next();
			double res = userFeature.dot(intVec.getVector());

			// first order
			res += userFeature.dot(alpha);
			res += cBetaIterator.next();

			IntDoublePairWritable min = queue.peek();
			if (null == min || queue.size() < TOP_N) {
				queue.add(new IntDoublePairWritable(intVec.getInt(), res));
			} else if (min.getValue() < res) {
				queue.poll();
				queue.add(new IntDoublePairWritable(intVec.getInt(), res));
			}
		}

	

		context.write(NullWritable.get(),
				new com.b5m.msgpack.PriorityQueue(new String(key.get()), queue));
	}
}
