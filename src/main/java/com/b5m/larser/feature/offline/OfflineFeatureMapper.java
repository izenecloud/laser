package com.b5m.larser.feature.offline;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

import com.b5m.larser.feature.RequestWritable;

public class OfflineFeatureMapper extends
		Mapper<Text, RequestWritable, Text, VectorWritable> {
	private static final Random RANDOM = new Random();

	protected void map(Text key, RequestWritable value, Context context)
			throws IOException, InterruptedException {
		if (-1 == value.getAction()) {
			if (Math.abs(RANDOM.nextInt() % 100) >= 1) {
				return;
			}
		}
		
		Vector userFeature = value.getUserFeature();
		Vector itemFeature = value.getItemFeature();
		int userDimension = userFeature.size();
		int itemDimension = itemFeature.size();
		Vector offlineFeature = new SequentialAccessSparseVector(userDimension
				+ itemDimension + userDimension * itemDimension + 1);

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
