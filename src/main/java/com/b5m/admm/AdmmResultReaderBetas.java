package com.b5m.admm;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import static com.b5m.admm.AdmmIterationHelper.*;

public class AdmmResultReaderBetas extends AdmmResultReader {

	@Override
	public Object read(JobConf conf, FileSystem fs, Path filePath)
			throws IOException {
		if (!fs.exists(filePath)) {
			return null;
		}
		for (Path file :getFilePaths(conf, fs, filePath)) {
			int inputSize = getFileLength(fs, file);
			if (0 >= inputSize) {
				continue;
			}
			FSDataInputStream in = fs.open(file);
			String jsonString = fsDataInputStreamToString(in, inputSize);
			return jsonToArray(jsonString);
		}
		return null;
	}
}
