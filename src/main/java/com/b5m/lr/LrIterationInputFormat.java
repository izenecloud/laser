package com.b5m.lr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class LrIterationInputFormat<K, V> extends SequenceFileInputFormat<K, V> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}
