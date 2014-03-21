package com.b5m.admm;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import static com.b5m.admm.AdmmIterationHelper.*;
import static com.b5m.admm.AdmmIterationHelper.*;

public class AdmmResultReaderBetas extends AdmmResultReader {

	@Override
	public Object read(JobConf conf, FileSystem fs, Path filePath)
			throws IOException {
		if (!fs.exists(filePath)) {
			return null;
		}
		for (Path file : getFilePaths(conf, fs, filePath)) {
			int inputSize = getFileLength(fs, file);
			if (0 >= inputSize) {
				continue;
			}
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			AdmmReducerContextWritable reduceContextWritable = new AdmmReducerContextWritable();
			reader.next(NullWritable.get(), reduceContextWritable);
			reader.close();

			AdmmReducerContext reduceContext = reduceContextWritable.get();
			return reduceContext.getZUpdated();
		}
		return null;
	}
}
