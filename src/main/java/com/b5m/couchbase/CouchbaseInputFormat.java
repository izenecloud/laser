package com.b5m.couchbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.vbucket.config.VBucket;

public class CouchbaseInputFormat extends InputFormat<BytesWritable, BytesWritable> {
	static class CouchbaseSplit extends InputSplit implements Writable {
		final List<Integer> vbuckets;

		CouchbaseSplit() {
			vbuckets = new ArrayList<Integer>();
		}
		
		CouchbaseSplit(List<Integer> vblist) {
			vbuckets = vblist;
		}

		public void readFields(DataInput in) throws IOException {
			short numvbuckets = in.readShort();
			for(int i = 0; i < numvbuckets; i++) {
				vbuckets.add(new Integer(in.readShort()));
			}
		}

		public void write(DataOutput out) throws IOException {
			out.writeShort(vbuckets.size());
			for(Integer v : vbuckets) {
				out.writeShort(v.shortValue());
			}
		}

		public long getLength() throws IOException {
			return vbuckets.size();
		}

		public String[] getLocations() {
			return new String[0];
		}
	}

	@Override
	public RecordReader<BytesWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		final CouchbaseRecordReader reader = new CouchbaseRecordReader();
		reader.initialize(split, context);
		return reader;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		final URI ClusterURI;
		try {
			ClusterURI = new URI(conf.get(CouchbaseConfig.CB_INPUT_CLUSTER));
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		final List<URI> ClientURIList = new ArrayList<URI>();
		ClientURIList.add(ClusterURI.resolve("/pools"));
		final String bucket = conf.get(CouchbaseConfig.CB_INPUT_BUCKET, "");
		final String password = conf.get(CouchbaseConfig.CB_INPUT_PASSWORD, "");

		final CouchbaseConnectionFactory fact = new CouchbaseConnectionFactory(
				ClientURIList, bucket, password);

		final com.couchbase.client.vbucket.config.Config vbconfig = fact
				.getVBucketConfig();

		final List<VBucket> allVBuckets = vbconfig.getVbuckets();
		@SuppressWarnings("unchecked")
		final ArrayList<Integer>[] vblists = 
				new ArrayList[vbconfig.getServersCount()];
		int vbid = 0;
		for(VBucket v : allVBuckets) {
			if(vblists[v.getMaster()] == null) {
				vblists[v.getMaster()] = new ArrayList<Integer>();
			}
			vblists[v.getMaster()].add(vbid);
			vbid++;
		}
		final ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		for(ArrayList<Integer> vblist : vblists) {
			splits.add(new CouchbaseSplit(vblist));
		}
		return splits;
	}

}
