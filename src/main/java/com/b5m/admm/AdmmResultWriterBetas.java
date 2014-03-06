package com.b5m.admm;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.jetbrains.annotations.TestOnly;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.b5m.admm.AdmmIterationHelper.*;

public class AdmmResultWriterBetas extends AdmmResultWriter {
	@Override
	public void write(JobConf conf, FileSystem hdfs, Path hdfsFilePath,
			Path finalOutputPath) throws IOException {
		Path[] files = getFilePaths(conf, hdfs, hdfsFilePath);
		for (Path file : files) {
			int inputSize = getFileLength(hdfs, file);
			if (inputSize <= 0) {
				continue;
			}
			FSDataInputStream in = hdfs.open(file);
			in.seek(0);
			writeBetas(in, inputSize, conf, file, finalOutputPath);
		}
	}

	private void writeBetas(FSDataInputStream in, int inputSize, JobConf conf,
			Path hdfsFilePath, Path finalOutputPath) throws IOException {
		String jsonString = fsDataInputStreamToString(in, inputSize);
		String betasString = buildBetasString(jsonString);
		InputStream inBetas = new ByteArrayInputStream(betasString.getBytes());

		Path betasPathFull = new Path(finalOutputPath, hdfsFilePath.getName());

		getFSAndWriteFile(conf, inBetas, betasPathFull);
	}

	@TestOnly
	public String buildBetasString(String jsonString) throws IOException {
		String betaString = jsonToMap(jsonString).values().iterator().next();

		AdmmMapperContext admmMapperContext = AdmmIterationHelper
				.jsonToAdmmMapperContext(betaString);

		double[] zInitials = admmMapperContext.getZInitial();
		return arrayToJson(zInitials);
	}

}
