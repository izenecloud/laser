package com.b5m.larser.feature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b5m.flume.B5MEvent;

public abstract class LaserMessageConsumer {
	private static final Logger LOG = LoggerFactory
			.getLogger(LaserMessageConsumer.class);

	private final Path output;
	private final FileSystem fs;
	private final Configuration conf;
	private SequenceFile.Writer writer;
	private long majorVersion = 0;
	private long minorVersion = 0;
	private final String collection;


	public LaserMessageConsumer(String collection, Path output, FileSystem fs, Configuration conf)
			throws IOException {
		this.collection = collection;
		this.output = output;
		this.fs = fs;
		this.conf = conf;

		initSequenceWriter();
	}

	public synchronized void shutdown() throws IOException {
		writer.close();
	}

	public abstract boolean write(B5MEvent b5mEvent) throws IOException;

	public abstract void flush() throws IOException;

	public synchronized void append(Text key, RequestWritable val) throws IOException {
		writer.append(key, val);
	}

	@SuppressWarnings("deprecation")
	public synchronized void incrMinorVersion() throws IOException {
		writer.close();

		minorVersion++;
		writer = SequenceFile.createWriter(
				fs,
				conf,
				new Path(output, Long.toString(majorVersion) + "-"
						+ Long.toString(minorVersion)), Text.class,
				RequestWritable.class);
	}

	public synchronized long getMinorVersion() {
		return minorVersion;
	}

	@SuppressWarnings("deprecation")
	public synchronized void incrMajorVersion() throws IOException {
		writer.close();

		majorVersion++;
		minorVersion = 0;
		writer = SequenceFile.createWriter(
				fs,
				conf,
				new Path(output, Long.toString(majorVersion) + "-"
						+ Long.toString(minorVersion)), Text.class,
				RequestWritable.class);

	}

	public synchronized long getMajorVersion() {
		return majorVersion;
	}
	
	public String getCollection() {
		return collection;
	}

	@SuppressWarnings("deprecation")
	private synchronized void initSequenceWriter() throws IOException {

		try {
			FileStatus[] files = fs.listStatus(output);

			for (FileStatus file : files) {
				String name = file.getPath().getName();
				String[] versions = name.split("-");
				if (versions.length != 2) {
					continue;
				}
				Long majorVersion = Long.valueOf(versions[0]);
				if (this.majorVersion < majorVersion) {
					this.majorVersion = majorVersion;
				}
				Long minorVersion = Long.valueOf(versions[1]);
				if (this.minorVersion < minorVersion) {
					this.minorVersion = minorVersion;
				}
			}
		} catch (IOException e) {
			LOG.debug("{} doesn't exist", output);
		}

		minorVersion++;
		Path sequentialPath = new Path(output, Long.toString(majorVersion)
				+ "-" + Long.toString(minorVersion));

		LOG.debug("writing data to {}", sequentialPath);
		writer = SequenceFile.createWriter(fs, conf, sequentialPath,
				Text.class, RequestWritable.class);
	}

}
