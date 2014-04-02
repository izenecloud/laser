package com.b5m.larser.feature.offline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

import com.b5m.larser.feature.RequestWritable;

public class OfflineFeatureMapper extends
		Mapper<IntWritable, RequestWritable, IntWritable, VectorWritable> {
	private Vector offlineFeature = null;
	private int userDimension;
	private int itemDimension;

	protected void map(IntWritable key, RequestWritable value, Context context)
			throws IOException, InterruptedException {
		Vector userFeature = value.getUserFeature();
		Vector itemFeature = value.getItemFeature();
		if (null == offlineFeature) {
			userDimension = userFeature.size();
			itemDimension = itemFeature.size();
			offlineFeature = new SequentialAccessSparseVector(userDimension
					+ itemDimension + userDimension * itemDimension + 1);
		}
		// first order
		for (Element e : userFeature.nonZeroes()) {
			offlineFeature.set(e.index(), e.get());
		}
		for (Element e : itemFeature.nonZeroes()) {
			offlineFeature.set(userDimension + e.index(), e.get());
		}
		// second order
		for (Element elementOfUser : userFeature.nonZeroes()) {
			for (Element elemetOfItem : itemFeature.nonZeroes()) {
				offlineFeature.set(elementOfUser.index() * elemetOfItem.index()
						+ userDimension + itemDimension, elementOfUser.get()
						* elemetOfItem.get());
			}

		}
		// action
		offlineFeature.set(offlineFeature.size() - 1, value.getAction());
		context.write(key, new VectorWritable(offlineFeature));
	}
}
