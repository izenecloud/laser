package com.b5m.larser.offline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.b5m.larser.offline.LaserOfflineHelper.*;

public class LaserOfflineTopNMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, PriorityQueueWritable> {
	private Vector firstOrderUserRes;
	private Vector firstOderItemRes;
	private int TOP_N;
	private List<PriorityQueue> queue;

	private static final Logger LOG = LoggerFactory
			.getLogger(LaserOfflineTopNMapper.class);

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Path userRes = new Path(
				conf.get("laser.offline.topN.driver.first.order.user.res"));
		FileSystem fs = userRes.getFileSystem(conf);
		LOG.info("Load user related info: {}", userRes);
		firstOrderUserRes = readVector(userRes, fs, conf);

		Path itemRes = new Path(
				conf.get("laser.offline.topN.driver.first.order.item.res"));
		LOG.info("Load item related info: {}", itemRes);
		firstOderItemRes = readVector(itemRes, fs, conf);

		TOP_N = conf.getInt("laser.offline.topN.driver.top.n", 100);

		LOG.info("user dimension: {}, top N: {}", firstOrderUserRes.size(),
				TOP_N);

		queue = new ArrayList<PriorityQueue>(firstOrderUserRes.size());
		for (int i = 0; i < firstOrderUserRes.size(); i++) {
			queue.add(i, new PriorityQueue());
		}

	}

	protected void map(IntWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		int itemId = key.get();
		double firstOderItemRes = this.firstOderItemRes.get(itemId);
		Vector secondOrderRes = value.get();
		for (Element e : secondOrderRes.nonZeroes()) {
			int userId = e.index();

			double res = e.get() + firstOrderUserRes.get(userId)
					+ firstOderItemRes;
			PriorityQueue queue = this.queue.get(userId);
			DoubleIntPairWritable min = queue.peek();
			if (null == min || queue.size() < TOP_N) {
				queue.add(new DoubleIntPairWritable(itemId, res));
			} else if (min.getValue() < res) {
				queue.poll();
				queue.add(new DoubleIntPairWritable(itemId, res));
			}
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (int i = 0; i < queue.size(); i++) {
			context.write(new IntWritable(i),
					new PriorityQueueWritable(queue.get(i)));
		}
	}
}
