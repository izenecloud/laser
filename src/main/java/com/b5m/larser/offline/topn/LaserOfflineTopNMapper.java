package com.b5m.larser.offline.topn;

import java.io.IOException;
import java.util.Iterator;
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
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.larser.feature.UserProfile;

import static com.b5m.HDFSHelper.readMatrix;
import static com.b5m.HDFSHelper.readVector;

public class LaserOfflineTopNMapper
		extends
		Mapper<BytesWritable, BytesWritable, NullWritable, com.b5m.msgpack.PriorityQueue> {
	private Vector delta;
	private Vector CBeta;
	private Matrix AC;
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
		this.delta = readVector(new Path(offlinePath, "delta"), fs, conf);
		Vector beta = readVector(new Path(offlinePath, "beta"), fs, conf);
		Matrix A = readMatrix(new Path(offlinePath, "A"), fs, conf);

		Matrix C = readMatrix(
				new Path(conf.get("com.b5m.laser.offline.topn.cluster")), fs,
				conf);

		LOG.info("Calculate A multiply C");
		AC = new DenseMatrix(C.numRows(), A.numRows());
		for (int j = 0; j < C.numRows(); j++) {
			Vector cj = C.viewRow(j);
			Vector acj = AC.viewRow(j);
			for (int i = 0; i < A.numRows(); i++) {
				Vector ai = A.viewRow(i);
				acj.set(i, ai.dot(cj));
			}
		}
		LOG.info("Calculate C multiply beta");
		CBeta = new DenseVector(C.numRows());
		for (int j = 0; j < C.numRows(); j++) {
			CBeta.set(j, C.viewRow(j).dot(beta));
		}

		TOP_N = conf.getInt("laser.offline.topn.n", 5);
		queue = new PriorityQueue(TOP_N);
	}

	protected void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		UserProfile user = UserProfile.createUserProfile(value.getBytes()
				.toString());
		user.setUserFeature(userFeature);

		for (int row = 0; row < this.AC.numRows(); row++) {
			double res = userFeature.dot(AC.viewRow(row));
			// first order
			res += userFeature.dot(delta);
			res += CBeta.get(row);

			IntDoublePairWritable min = queue.peek();
			if (null == min || queue.size() < TOP_N) {
				queue.add(new IntDoublePairWritable(row, res));
			} else if (min.getValue() < res) {
				queue.poll();
				queue.add(new IntDoublePairWritable(row, res));
			}
		}

		Map<Integer, Double> topRes = new HashedMap(TOP_N);
		Iterator<IntDoublePairWritable> iterator = queue.iterator();
		while (iterator.hasNext()) {
			IntDoublePairWritable v = iterator.next();
			topRes.put(v.getKey(), v.getValue());
		}

		context.write(NullWritable.get(),
				new com.b5m.msgpack.PriorityQueue(key.toString(), topRes));
	}
}
