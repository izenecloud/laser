package com.b5m.larser.online;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.b5m.lr.ListWritable;

public class OnlineTrainMapper extends
		Mapper<Writable, Writable, IntWritable, ListWritable> {
	private static final Random random = new Random();

	private int itemsPerMapTask;
	private int FEATRUE_DIMENSION;
	private int SMAPLE_DIMENSION_PER_ITEM;
	private int NON_ZERO_PER_FEATURE;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		itemsPerMapTask = conf.getInt(
				"com.b5m.laser.online.sample.per.mapTask", 0);
		FEATRUE_DIMENSION = conf.getInt(
				"com.b5m.laser.online.feature.dimension", 0);
		NON_ZERO_PER_FEATURE = conf.getInt(
				"com.b5m.laser.online.none.zero.per.feature", 0);
		SMAPLE_DIMENSION_PER_ITEM = conf.getInt(
				"com.b5m.laser.online.sample.dimension.per.item", 0);
	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		for (int i = 0; i < itemsPerMapTask; i++) {
			int itemId = random.nextInt();
			Vector args = new DenseVector(FEATRUE_DIMENSION);
			for (int j = 0; j < FEATRUE_DIMENSION; j++) {
				args.set(j, random.nextDouble());
			}

			List<Writable> itemData = new LinkedList<Writable>();
			for (int j = 0; j < SMAPLE_DIMENSION_PER_ITEM; j++) {
				Vector row = new SequentialAccessSparseVector(
						FEATRUE_DIMENSION + 1);
				for (int k = 0; k < NON_ZERO_PER_FEATURE; k++) {
					int index = Math.abs(random.nextInt() % FEATRUE_DIMENSION);
					row.set(index, random.nextDouble() * args.get(index));
				}
				if (random.nextInt() % 1000 < 5) {
					row.set(FEATRUE_DIMENSION, 1);
				} else {
					row.set(FEATRUE_DIMENSION, -1);
				}
				itemData.add(new VectorWritable(row));
			}
			context.write(new IntWritable(itemId), new ListWritable(itemData));

		}
	}
}
