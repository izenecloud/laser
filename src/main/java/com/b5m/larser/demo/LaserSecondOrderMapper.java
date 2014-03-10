package com.b5m.larser.demo;

import java.io.IOException;
import java.util.logging.Level;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

public class LaserSecondOrderMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	private Matrix A;
	private static final Pattern TAB_PATTERN = Pattern.compile("\t");
	private static final Pattern COLON_PATTERN = Pattern.compile(":");
	private static final Integer COL = 500000;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Path ap = new Path(conf.get("laser.second.order.a"));
		FileSystem fs = ap.getFileSystem(conf);
		FSDataInputStream input = fs.open(ap);
		A = MatrixWritable.readMatrix(input);
	}

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//System.out.println(value.toString());
		Vector v = createVectorFromDataString(value.toString());
		String Ac = new String();
		for (int row = 0; row < A.numRows(); row++) {
			Vector Ar = A.viewRow(row);
			double val = 0.0;
			for (Element e : v.nonZeroes()) {
				val += Ar.get(e.index()) * e.get();
			}
			Ac += Double.toString(val) + "\t";
		}
		context.write(key, new Text(Ac));
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	public Vector createVectorFromDataString(String dataString) {
		Vector v = new RandomAccessSparseVector(COL);
		String[] elements = TAB_PATTERN.split(dataString);
		for (int j = 0; j < elements.length - 1; ++j) {
			String[] element = COLON_PATTERN.split(elements[j]);
			Integer featureId = Integer.parseInt(element[0]);
			Double feature = Double.parseDouble(element[1]);
			v.set(featureId, feature);
		}
		return v;
	}
}
