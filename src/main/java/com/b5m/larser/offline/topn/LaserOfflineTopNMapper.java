package com.b5m.larser.offline.topn;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.b5m.larser.feature.UserProfile;
import com.b5m.larser.feature.UserProfileHelper;
import com.b5m.msgpack.ClusteringInfo;
import com.b5m.msgpack.ClusteringInfoResponse;

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

	private UserProfileHelper helper;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String offlineModel = conf
				.get("com.b5m.laser.offline.topn.offline.model");
		Path offlinePath = new Path(offlineModel);
		FileSystem fs = offlinePath.getFileSystem(conf);
		// TODO getLocalCacheFiles
		this.alpha = readVector(new Path(offlinePath, "alpha"), fs, conf);
		Vector beta = readVector(new Path(offlinePath, "beta"), fs, conf);
		Matrix A = readMatrix(new Path(offlinePath, "A"), fs, conf);

		AC = new LinkedList<IntVector>();
		CBeta = new LinkedList<Double>();

		ClusteringInfoResponse response = null;
		{
			Path clusteringPath = new Path(
					conf.get("com.b5m.laser.offline.topn.clustering.info"));
			DataInputStream in = fs.open(clusteringPath);
			try {
				response = ClusteringInfoResponse.read(in);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		Iterator<ClusteringInfo> iterator = response.iterator();
		while (iterator.hasNext()) {
			ClusteringInfo info = iterator.next();
			com.b5m.larser.offline.topn.ClusterInfo cluster = new com.b5m.larser.offline.topn.ClusterInfo(
					info.clusteringIndex, info.pows, beta.size());
			// A * Cj
			Vector acj = new DenseVector(A.numRows());
			for (int row = 0; row < A.numRows(); row++) {
				Vector ai = A.viewRow(row);
				acj.set(row, cluster.getClusterFeatureVector().dot(ai));
			}
			AC.add(new IntVector(cluster.getClusterHash(), acj));

			// Cj * beta
			CBeta.add(cluster.getClusterFeatureVector().dot(beta));
		}

		userFeature = new SequentialAccessSparseVector(A.numRows());

		TOP_N = conf.getInt("laser.offline.topn.n", 5);
		queue = new PriorityQueue(TOP_N);

		helper = null;
		{
			Path serializePath = new Path(
					conf.get("com.b5m.laser.offline.topn.user.feature.map"));
			// Path serializePath = context.getLocalCacheFiles()[0];
			DataInputStream in = fs.open(serializePath);
			try {
				helper = UserProfileHelper.read(in);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			in.close();
		}
	}

	protected void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		queue.clear();
		String json = new String(value.get());
		if (json.length() <= 8)
			return;
		UserProfile user = UserProfile
				.createUserProfile(json);
		user.setUserFeature(userFeature, helper, false);

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

		context.write(NullWritable.get(), new com.b5m.msgpack.PriorityQueue(
				new String(key.get()), queue));
	}
}
