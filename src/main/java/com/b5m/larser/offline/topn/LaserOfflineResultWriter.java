package com.b5m.larser.offline.topn;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;


import com.b5m.admm.AdmmReducerContext;
import com.b5m.admm.AdmmReducerContextWritable;
import com.b5m.msgpack.MsgpackClient;

import static com.b5m.HDFSHelper.*;

public class LaserOfflineResultWriter {

	public void write(String collection, Boolean direct, FileSystem fs,
			Path hdfsFilePath) throws Exception {
		if (!fs.exists(hdfsFilePath)) {
			return;
		}

		int ufDimension = com.b5m.conf.Configuration.getInstance()
				.getUserFeatureDimension(collection);
		int adfDimension = com.b5m.conf.Configuration.getInstance()
				.getItemFeatureDimension(collection);
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, hdfsFilePath,
				conf);
		AdmmReducerContextWritable reduceContextWritable = new AdmmReducerContextWritable();
		reader.next(NullWritable.get(), reduceContextWritable);
		reader.close();
		AdmmReducerContext reduceContext = reduceContextWritable.get();
		double[] z = reduceContext.getZUpdated();

		if (!direct) {

			Path finalOutputPath = com.b5m.conf.Configuration.getInstance()
					.getLaserOfflineOutput(collection);

			Vector alpha = new DenseVector(ufDimension);
			for (int i = 0; i < ufDimension; i++) {
				alpha.set(i, z[i]);
			}
			Vector beta = new DenseVector(adfDimension);
			for (int i = ufDimension; i < ufDimension + adfDimension; i++) {
				beta.set(i - ufDimension, z[i]);
			}
			Matrix A = new DenseMatrix(ufDimension, adfDimension);
			for (int row = 0; row < A.numRows(); row++) {
				Vector v = A.viewRow(row);
				for (int col = 0; col < A.numCols(); col++) {
					v.set(col, z[row * adfDimension + col + ufDimension
							+ adfDimension]);
				}
			}

			writeVector(alpha, new Path(finalOutputPath, "alpha"), fs, conf);
			writeVector(beta, new Path(finalOutputPath, "beta"), fs, conf);
			writeMatrix(A, new Path(finalOutputPath, "A"), fs, conf);

		} else {
			final MsgpackClient client = new MsgpackClient(com.b5m.conf.Configuration
					.getInstance().getMsgpackAddress(collection),
					com.b5m.conf.Configuration.getInstance().getMsgpackPort(
							collection), collection);
			Object[] req = new Object[3];
			List<Float> alpha = new ArrayList<Float>(ufDimension);

			for (int i = 0; i < ufDimension; i++) {
				alpha.add((float) (z[i]));
			}
			req[0] = alpha;

			List<Float> beta = new ArrayList<Float>(adfDimension);
			for (int i = ufDimension; i < ufDimension + adfDimension; i++) {
				beta.add((float) z[i]);
			}
			req[1] = beta;

			List<Float> quadratic = new ArrayList<Float>(ufDimension
					* adfDimension);
			for (int i = ufDimension + adfDimension; i < z.length; i++) {
				quadratic.add((float) z[i]);
			}
			req[2] = quadratic;
			client.write(req, "updateLaserOfflineModel");
		}
	}
}
