package io.izenecloud.larser.offline.precompute;

import static io.izenecloud.HDFSHelper.readMatrix;
import static io.izenecloud.HDFSHelper.readVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

public class Mapper extends
		org.apache.hadoop.mapreduce.Mapper<Long, AdFeature, Long, Result> {
	private Vector beta = null;
	private Matrix A = null;
	private Vector advec = null;
	private List<Float> AStable = null;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String offlineModel = conf.get("com.b5m.laser.offline.model");
		Path offlinePath = new Path(offlineModel);
		FileSystem fs = offlinePath.getFileSystem(conf);
		beta = readVector(new Path(offlinePath, "beta"), fs, conf);
		A = readMatrix(new Path(offlinePath, "A"), fs, conf);
		advec = new SequentialAccessSparseVector(A.numCols());
		AStable = new ArrayList<Float>(A.numRows());
	}

	protected void map(Long key, AdFeature sv, Context context)
			throws IOException, InterruptedException {
		advec.assign(0);
		AStable.clear();
		double betaStable = 0.0; // ad * beta
		while (sv.v.hasNext()) {
			int index = sv.v.getIndex();
			float val = sv.v.get();
			betaStable += beta.get(index) * val;
			advec.setQuick(index, val);
		}
		for (int row = 0; row < A.numRows(); row++) {
			AStable.add((float) A.viewRow(row).dot(advec));
		}
		//System.gc();
		context.write(new Long(sv.k), new Result((float) betaStable, AStable));
	}
}
