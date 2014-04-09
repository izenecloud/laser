package com.b5m.larser.offline;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

public class LaserSecondOrderMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
	private Matrix args;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Path ap = new Path(conf.get("laser.second.order.args"));
		FileSystem fs = ap.getFileSystem(conf);
		FSDataInputStream input = fs.open(ap);
		args = MatrixWritable.readMatrix(input);
	}

	protected void map(IntWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		Vector res = new DenseVector(args.numRows());
		for (int row = 0; row < args.numRows(); row++) {
			Vector Ar = args.viewRow(row);
			double val = 0.0;
			//TODO choose most sparse
			for (Element e : value.get().nonZeroes()) {
				val += Ar.get(e.index()) * e.get();
			}
			res.set(row, val);
			// Ac += String.format("%1$.12f", val) + "\t";
			// Ac += Double.toString(val) + "\t";
		}
		context.write(key, new VectorWritable(res));
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}
}
