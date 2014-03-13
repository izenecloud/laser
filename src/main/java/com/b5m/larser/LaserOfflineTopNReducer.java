package com.b5m.larser;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class LaserOfflineTopNReducer
		extends
		Reducer<IntWritable, PriorityQueueWritable, IntWritable, PriorityQueueWritable> {

	private int TOP_N;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		TOP_N = context.getConfiguration().getInt(
				"laser.offline.topN.driver.top.n", 100);
	}

	protected void reduce(IntWritable key,
			Iterable<PriorityQueueWritable> values, Context context)
			throws IOException, InterruptedException {
		PriorityQueue res = null;
		for (PriorityQueueWritable queue : values) {
			if (null == res) {
				res = queue.get();
			} else {
				PriorityQueue q = queue.get();
				Iterator<DoubleIntPairWritable> iterator = q.iterator();
				while (iterator.hasNext()) {
					DoubleIntPairWritable it = iterator.next();
					DoubleIntPairWritable min = res.peek();
					if (null == min || res.size() < TOP_N) {
						res.add(it);
					} else if (min.getValue() < it.getValue()) {
						res.poll();
						res.add(it);
					}
				}
			}
		}
		context.write(key, new PriorityQueueWritable(res));
	}
}
