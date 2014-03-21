package com.b5m.admm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class AdmmIterationInputFormat<K, V> extends
		SequenceFileInputFormat<K, V> {
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	private static final double SPLIT_SLOP = 1.1; // 10% slop
	private static final long JAVA_OPTS = (long) 1 * 1024 * 1024 * 1024;

	protected long computeSplitSize(long javaOpts, long numMapTasks,
			long goalSize) {
		return Math.min(goalSize / numMapTasks, javaOpts);
	}

	public static void setNumMapTasks(JobContext job, int numMapTasks) {
		job.getConfiguration().setInt("admm.iteration.num.map.tasks",
				numMapTasks);
	}

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		int numMapTasks = conf.getInt("admm.iteration.num.map.tasks", 0);
		if (0 == numMapTasks) {
			return super.getSplits(job);
		}

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		long goalSize = 0;
		for (FileStatus file : files) {
			goalSize += file.getLen();
		}

		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					length);
			if ((length != 0) && isSplitable(job, path)) {
				long splitSize = computeSplitSize(JAVA_OPTS, numMapTasks,
						goalSize);
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
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}

		// Save the number of input files in the job-conf
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		job.getConfiguration().setInt("admm.iteration.num.map.tasks",
				splits.size());
		return splits;
	}

}