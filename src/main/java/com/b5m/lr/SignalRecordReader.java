package com.b5m.lr;

import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.google.common.base.Charsets;

public class SignalRecordReader extends LineRecordReader {
	private final static byte[] recordDelimiterBytes = new String("\r")
			.getBytes(Charsets.UTF_8);

	public SignalRecordReader() {
		super(recordDelimiterBytes);
	}
}
