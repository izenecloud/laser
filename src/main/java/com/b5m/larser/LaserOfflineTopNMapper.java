package com.b5m.larser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

public class LaserOfflineTopNMapper extends
		Mapper<IntWritable, VectorWritable, IntDoublePairWritable, IntWritable> {
	private Vector firstOrderUserRes;
	private Vector firstOderItemRes;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Path userRes = new Path(
				conf.get("laser.offline.topN.driver.first.order.user.res"));
		FileSystem fs = userRes.getFileSystem(conf);
		for (FileStatus file : fs.listStatus(userRes)) {
			if (0 < file.getLen()) {
				FSDataInputStream in = fs.open(file.getPath());
				firstOrderUserRes = VectorWritable.readVector(in);
				in.close();
				break;
			}
		}

		for (FileStatus file : fs.listStatus(new Path(conf
				.get("laser.offline.topN.driver.first.order.item.res")))) {
			if (0 < file.getLen()) {
				System.out.println(file.getPath());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), conf);
				VectorWritable val = new VectorWritable();
				reader.next(NullWritable.get(), val);
				firstOderItemRes = val.get();
				//firstOderItemRes = VectorWritable.readVector(in);
				//in.close();
				break;
			}
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
			context.write(new IntDoublePairWritable(userId, res),
					new IntWritable(itemId));
		}
	}
}
