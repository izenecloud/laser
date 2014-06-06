package com.b5m.larser.feature.online;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

import com.b5m.larser.feature.Request;

public class OnlineFeatureMapper extends
		Mapper<Text, Request, Text, VectorWritable> {

	protected void map(Text key, Request value, Context context)
			throws IOException, InterruptedException {
		Vector itemFeature = value.getItemFeature();

		// 0th - delta
		// 1th - eta
		Vector onlineFeature = new SequentialAccessSparseVector(
				itemFeature.size() + 1);
		for (Element e : itemFeature.nonZeroes()) {
			onlineFeature.set(e.index() + 1, e.get());
		}
		onlineFeature.set(0, 1);
		onlineFeature.set(itemFeature.size(), value.getAction());
		
		context.write(key, new VectorWritable(onlineFeature));
	}
}
