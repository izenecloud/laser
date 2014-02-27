package com.b5m.admm;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class AdmmResultWriterIteration extends AdmmResultWriter {

	@Override
	public void write(JobConf conf, FileSystem hdfs, Path hdfsFilePath,
			Path finalOutputPath) throws IOException {
		Path[] files = getFilePaths(conf, hdfs, hdfsFilePath);
		for (Path file : files) {
			Path finalOutputPathFull = new Path(finalOutputPath,
					file.getName());
			FSDataInputStream in = hdfs.open(file);
			getFSAndWriteFile(conf, in, finalOutputPathFull);
		}
	}

}