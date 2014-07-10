package com.b5m.larser.online;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.b5m.conf.Configuration;
import com.b5m.lr.ListWritable;
import com.b5m.lr.LrIterationDriver;

public class LaserOnlineModelTrainerIT {
	private static final String PROPERTIES = "src/test/properties/laser.properties.examble";
	private static final Random random = new Random();
	private static final Integer ITEM_DIMENSION = 10; // 10k
	private static final Integer NON_ZERO_PER_FEATURE = 100;
	private static final Integer FEATRUE_DIMENSION = 1000000; // 1M
	private static final Integer SMAPLE_DIMENSION_PER_ITEM = 1000; // 1K
	private FileSystem fs;
	private org.apache.hadoop.conf.Configuration conf;

//	@BeforeTest
//	public void setup() throws IOException {
//		Path pro = new Path(PROPERTIES);
//		conf = new org.apache.hadoop.conf.Configuration();
//
//		fs = pro.getFileSystem(conf);
//		Configuration.getInstance().load(pro, fs);
//
//		Path metaqOutput = Configuration.getInstance().getMetaqOutput();
//		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
//				new Path(metaqOutput, "1"), Text.class, ListWritable.class);
//
//		for (int i = 0; i < ITEM_DIMENSION; i++) {
//			int itemId = random.nextInt();
//			Vector args = new DenseVector(FEATRUE_DIMENSION);
//			for (int j = 0; j < FEATRUE_DIMENSION; j++) {
//				args.set(j, random.nextDouble());
//			}
//
//			List<Writable> itemData = new LinkedList<Writable>();
//			for (int j = 0; j < SMAPLE_DIMENSION_PER_ITEM; j++) {
//				Vector row = new SequentialAccessSparseVector(
//						FEATRUE_DIMENSION + 1);
//
//				for (int k = 0; k < NON_ZERO_PER_FEATURE; k++) {
//					int index = Math.abs(random.nextInt() % FEATRUE_DIMENSION);
//					row.set(index, random.nextDouble() * args.get(index));
//				}
//				if (random.nextInt() % 1000 < 5) {
//					row.set(FEATRUE_DIMENSION, 1);
//				} else {
//					row.set(FEATRUE_DIMENSION, -1);
//				}
//				itemData.add(new VectorWritable(row));
//			}
//			writer.append(new Text(Integer.toString(itemId)), new ListWritable(
//					itemData));
//		}
//		writer.close();
//	}
//
//	@Test
//	public void test() throws ClassNotFoundException, IOException,
//			InterruptedException {
//		LrIterationDriver.run(Configuration.getInstance().getMetaqOutput(),
//				null, Configuration.getInstance().getRegularizationFactor(),
//				Configuration.getInstance().addIntercept(), conf);
//	}
//
//	@AfterTest
//	public void close() throws IOException {
//		fs.delete(Configuration.getInstance().getMetaqOutput(), true);
//	}
}
