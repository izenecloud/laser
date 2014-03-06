package com.b5m.admm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public abstract class AdmmResultReader {
	public abstract Object read(JobConf conf, FileSystem fs, Path filePath)
			throws IOException;
	
	protected Path[] getFilePaths(JobConf conf, FileSystem fs, Path filePath) throws IOException {
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
}
