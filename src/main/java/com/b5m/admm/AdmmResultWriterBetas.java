package com.b5m.admm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AdmmResultWriterBetas extends AdmmResultWriter {
	@Override
	public void write(Configuration conf, FileSystem hdfs, Path hdfsFilePath,
			Path finalOutputPath) throws IOException {
		Path beta = new Path(hdfsFilePath, "Z");
		FSDataInputStream in = beta.getFileSystem(conf).open(beta);
		getFSAndWriteFile(conf, in, finalOutputPath);
	}
}
