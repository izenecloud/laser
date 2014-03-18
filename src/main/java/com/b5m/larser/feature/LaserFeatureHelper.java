package com.b5m.larser.feature;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;

public class LaserFeatureHelper {
	public static IntDoublePair createKeyValuePair(String key, String value) {
		//TODO
		return new IntDoublePair(Integer.valueOf(key), Double.valueOf(value));
	}
	
	public static void deleteFiles(Path path, String filePattern, FileSystem fs) throws IOException {
		 FileStatus[] fileStatus = fs.listStatus(path, new GlobFilter(filePattern));
		 for (FileStatus file : fileStatus) {
			 fs.delete(file.getPath());
		 }
	}
}
