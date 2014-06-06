package com.b5m.larser.feature;

import static com.b5m.HDFSHelper.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.flume.B5MEvent;

public abstract class LaserMessageConsumer {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserMessageConsumer.class);
	private static final String OFFLINE_FOLDER = "OFFLINE_FOLDER";
	private static final String ONLINE_FOLDER = "ONLINE_FOLDER";

	private final Path output;
	private final FileSystem fs;
	private final Configuration conf;
	private SequenceFile.Writer offlineWriter;
	private SequenceFile.Writer onlineWriter;
	private Vector alpha = null;
	private Vector beta = null;
	private Matrix quadratic = null;
	private long offlineVersion = 0;
	private long onlineVersion = 0;
	private final String collection;

	public LaserMessageConsumer(String collection, Path output, FileSystem fs,
			Configuration conf) throws IOException {
		this.collection = collection;
		this.output = output;
		this.fs = fs;
		this.conf = conf;

		Path onlinePath = new Path(output, ONLINE_FOLDER + "/"
				+ Long.toString(onlineVersion));
		onlineWriter = SequenceFile.createWriter(fs, conf, onlinePath,
				Text.class, OnlineVectorWritable.class);
		Path offlinePath = new Path(output, ONLINE_FOLDER + "/"
				+ Long.toString(offlineVersion));
		offlineWriter = SequenceFile.createWriter(fs, conf, offlinePath,
				Text.class, VectorWritable.class);
	}

	public synchronized void shutdown() throws IOException {
		offlineWriter.close();
		onlineWriter.close();
	}

	public void loadOfflineMode() {
		synchronized (alpha) {

			try {
				Path offlineModel = com.b5m.conf.Configuration.getInstance()
						.getLaserOfflineOutput(collection);
				alpha = readVector(new Path(offlineModel, "alpha"), fs, conf);
				beta = readVector(new Path(offlineModel, "beta"), fs, conf);
				quadratic = readMatrix(new Path(offlineModel, "A"), fs, conf);
			} catch (Exception e) {
				LOG.info("offline model does not exist, {}");
			}
		}
	}

	public abstract boolean write(B5MEvent b5mEvent) throws IOException;

	public abstract void flush() throws IOException;

	public void appendOnline(Text key, OnlineVectorWritable val)
			throws IOException {
		synchronized (onlineWriter) {
			onlineWriter.append(key, val);
		}
	}

	public void appendOffline(Text key, Request value) throws IOException {
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
		synchronized (offlineWriter) {
			offlineWriter.append(key, new VectorWritable(offlineFeature));
		}
	}

	public double knownOffset(Request value) throws IOException {
		synchronized (alpha) {
			if (alpha == null || beta == null || quadratic == null) {
				return 0;
			}
			Vector userFeature = value.getUserFeature();
			double offset = alpha.dot(userFeature);
			Vector itemFeature = value.getItemFeature();
			offset += beta.dot(itemFeature);

			for (int row = 0; row < quadratic.numRows(); row++) {
				offset += userFeature.get(row)
						* quadratic.viewRow(row).dot(itemFeature);
			}
			return offset;
		}
	}

	public Path nextOnlinePath() throws IOException {
		synchronized (onlineWriter) {
			onlineWriter.close();
			Path ret = new Path(output, ONLINE_FOLDER + "/"
					+ Long.toString(onlineVersion));
			onlineVersion++;
			Path onlinePath = new Path(output, ONLINE_FOLDER + "/"
					+ Long.toString(onlineVersion));
			LOG.info("Update online feature output path, to {}", onlinePath);
			onlineWriter = SequenceFile.createWriter(fs, conf, onlinePath,
					Text.class, OnlineVectorWritable.class);
			return ret;
		}
	}

	public Path nextOfflinePath() throws IOException {
		synchronized (offlineWriter) {
			offlineWriter.close();
			Path ret = new Path(output, OFFLINE_FOLDER + "/"
					+ Long.toString(offlineVersion));
			offlineVersion++;
			Path offlinePath = new Path(output, ONLINE_FOLDER + "/"
					+ Long.toString(offlineVersion));
			LOG.info("Update offline feature output path, to {}", offlinePath);

			offlineWriter = SequenceFile.createWriter(fs, conf, offlinePath,
					Text.class, VectorWritable.class);
			return ret;
		}
	}

	public String getCollection() {
		return collection;
	}
}
