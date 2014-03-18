package com.b5m.larser.feature;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.b5m.flume.B5MEvent;

public class LaserFeatureRecordReader extends
		RecordReader<LongWritable, B5MEvent> {
	DatumReader<B5MEvent> datumReader;
	DataFileReader<B5MEvent> dataFileReader;
	private LongWritable key = null;
	private B5MEvent value = null;
	private long start = 0;
	private long end = 0;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		datumReader = new SpecificDatumReader<B5MEvent>(B5MEvent.SCHEMA$);
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		SeekableInput input = new FsInput(split.getPath(), job);
		dataFileReader = new DataFileReader<B5MEvent>(input, datumReader);
		dataFileReader.sync(split.getStart());
		start = dataFileReader.tell();
		end = split.getStart() + split.getLength();		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		//if (value == null) {
		//	value = new B5MEvent();
		//}
		if (dataFileReader.hasNext()) {
			value = dataFileReader.next();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public B5MEvent getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		long pos = dataFileReader.tell();
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		dataFileReader.close();
	}

}
