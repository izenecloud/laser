package com.b5m.larser.offline.topn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.b5m.admm.AdmmReducerContext;
import com.b5m.admm.AdmmReducerContextWritable;

import static com.b5m.HDFSHelper.*;

public class LaserOfflineResultWriter {
//	private static final Logger LOG = LoggerFactory
//			.getLogger(LaserOfflineResultWriter.class);

	public void write(Configuration conf, FileSystem hdfs, Path hdfsFilePath,
			Path finalOutputPath) throws IOException {
		if (!hdfs.exists(hdfsFilePath)) {
			return;
		}

		SequenceFile.Reader reader = new SequenceFile.Reader(hdfs,
				hdfsFilePath, conf);
		AdmmReducerContextWritable reduceContextWritable = new AdmmReducerContextWritable();
		reader.next(NullWritable.get(), reduceContextWritable);
		reader.close();

		AdmmReducerContext reduceContext = reduceContextWritable.get();
		double[] z = reduceContext.getZUpdated();
		int userDimension = com.b5m.conf.Configuration.getInstance()
				.getUserFeatureDimension();
		int itemDimension = com.b5m.conf.Configuration.getInstance()
				.getItemFeatureDimension();
		{
			Vector alpha = new DenseVector(userDimension);
			for (int i = 0; i < userDimension; i++) {
				alpha.set(i, z[i]);
			}
			Vector beta = new DenseVector(itemDimension);
			for (int i = userDimension; i < userDimension + itemDimension; i++) {
				beta.set(i - userDimension, z[i]);
			}
			Matrix A = new DenseMatrix(userDimension, itemDimension);
			for (int row = 0; row < A.numRows(); row++) {
				Vector v = A.viewRow(row);
				for (int col = 0; col < A.numCols(); col++) {
//					System.out.println(row * itemDimension + col
//							+ userDimension + itemDimension);
					v.set(col, z[row * itemDimension + col + userDimension
							+ itemDimension]);
				}
			}

			writeVector(alpha, new Path(finalOutputPath, "alpha"), hdfs, conf);
			writeVector(beta, new Path(finalOutputPath, "beta"), hdfs, conf);
			writeMatrix(A, new Path(finalOutputPath, "A"), hdfs, conf);
		}
	}
}
