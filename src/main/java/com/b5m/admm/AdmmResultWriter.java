package com.b5m.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class AdmmResultWriter {
	public abstract void write(Configuration conf, FileSystem hdfs,
			Path hdfsFilePath, Path finalOutputPath) throws IOException;

	protected Path[] getFilePaths(Configuration conf, FileSystem fs,
			Path filePath) throws IOException {
		FileStatus[] hdfsFiles = fs.listStatus(filePath);
		Path[] hdfsFilePaths = FileUtil.stat2Paths(hdfsFiles);
		List<Path> files = new ArrayList<Path>();
		for (Path hdfsFilePath : hdfsFilePaths) {
			FileStatus fileStatus = fs.getFileStatus(hdfsFilePath);
			if (!fileStatus.isDir()) {
				files.add(hdfsFilePath);
			}
		}
		return files.toArray(new Path[0]);
	}

	protected void getFSAndWriteFile(Configuration conf, InputStream in,
			Path finalOutputPathFull) throws IOException {
		FileSystem fs = finalOutputPathFull.getFileSystem(conf);
		if (fs.exists(finalOutputPathFull)) {
			fs.delete(finalOutputPathFull, true);
		}

		FSDataOutputStream out = fs.create(finalOutputPathFull);
		IOUtils.copyBytes(in, out, conf, true);
		out.close();
	}
}