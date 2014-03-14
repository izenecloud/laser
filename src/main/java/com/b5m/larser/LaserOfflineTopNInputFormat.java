package com.b5m.larser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class LaserOfflineTopNInputFormat<K, V> extends
		SequenceFileInputFormat<K, V> {
	private static final Log LOG = LogFactory
			.getLog(LaserOfflineTopNInputFormat.class);

	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	private static final double SPLIT_SLOP = 1.1; // 10% slop

	protected long computeSplitSize(long blockSize, long numMapTasks,
			long goalSize) {
		return goalSize / numMapTasks;
	}

	public static void setNumMapTasks(JobContext job, long numMapTasks) {
		job.getConfiguration().setLong("laser.offline.topn.num.map.tasks",
				numMapTasks);
	}

	public List<InputSplit> getSplits(JobContext job) throws IOException {
//		long maxSize = getMaxSplitSize(job);
		long numMapTasks = job.getConfiguration().getLong(
				"laser.offline.topn.num.map.tasks", 10);
//		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		long goalSize = 0;
		for (FileStatus file : files) {
//			Path path = file.getPath();
			goalSize += file.getLen();
		}
		
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					length);
			if ((length != 0) && isSplitable(job, path)) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, numMapTasks,
						goalSize);

//				System.out.println(Long.toString(splitSize)
//						+ "\t"
//						+ Long.toString(super.computeSplitSize(blockSize,
//								minSize, maxSize)));
				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length
							- bytesRemaining);
					splits.add(new FileSplit(path, length - bytesRemaining,
							splitSize, blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplit(path, length - bytesRemaining,
							bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts()));
				}
			} else if (length != 0) {
				splits.add(new FileSplit(path, 0, length, blkLocations[0]
						.getHosts()));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}

		// Save the number of input files in the job-conf
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

		LOG.debug("Total # of splits: " + splits.size());
		return splits;
	}
}
