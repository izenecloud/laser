package io.izenecloud.couchbase;

import io.izenecloud.couchbase.CouchbaseInputFormat.CouchbaseSplit;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import net.spy.memcached.tapmessage.RequestMessage;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapMagic;
import net.spy.memcached.tapmessage.TapOpcode;
import net.spy.memcached.tapmessage.TapRequestFlag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.couchbase.client.TapClient;

public class CouchbaseRecordReader extends RecordReader<BytesWritable, BytesWritable> {
	TapClient tapclient;
	ResponseMessage lastmessage;

	public void close() throws IOException {
		tapclient.shutdown();
	}
	
	public BytesWritable getCurrentKey() throws IOException {
		if (lastmessage == null)
			return null;
		return new BytesWritable(lastmessage.getKey().getBytes());
	}

	public BytesWritable getCurrentValue() throws IOException {
		if (lastmessage == null)
			return null;
		return  new BytesWritable(lastmessage.getValue());
	}

	public float getProgress() {
		if (!tapclient.hasMoreMessages()) {
			return 1;
		}
		return 0;
	}

	public boolean nextKeyValue() {
		while (tapclient.hasMoreMessages()) {
			lastmessage = tapclient.getNextMessage(1, TimeUnit.SECONDS);
			if (lastmessage != null) {
				return true;
			}
		}
		return false;
	}

	public BytesWritable createKey() {
		return new BytesWritable();
	}

	public BytesWritable createValue() {
		return new BytesWritable();
	}

	public long getPos() throws IOException {
		//TODO
		return 0;
	}

	public boolean next(BytesWritable k, BytesWritable v)
			throws IOException {
		if(nextKeyValue()) {
			k.set(getCurrentKey());
			v.set(getCurrentValue());
			return true;
		}
		return false;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		final URI ClusterURI;
		try {
			ClusterURI = new URI(conf.get(CouchbaseConfig.CB_INPUT_CLUSTER));
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		final List<URI> ClientURIList = new ArrayList<URI>();
		ClientURIList.add(ClusterURI.resolve("/pools"));
		final String bucket = conf.get(CouchbaseConfig.CB_INPUT_BUCKET,
				"");
		final String password = conf.get(CouchbaseConfig.CB_INPUT_PASSWORD, "");

		RequestMessage tapReq = new RequestMessage();
		tapReq.setMagic(TapMagic.PROTOCOL_BINARY_REQ);
		tapReq.setOpcode(TapOpcode.REQUEST);
		tapReq.setFlags(TapRequestFlag.DUMP);
		tapReq.setFlags(TapRequestFlag.SUPPORT_ACK);
		tapReq.setFlags(TapRequestFlag.FIX_BYTEORDER);
		tapReq.setFlags(TapRequestFlag.LIST_VBUCKETS);
		final CouchbaseSplit couchSplit = (CouchbaseSplit) split;
		short[] vbids = new short[couchSplit.vbuckets.size()];
		int i = 0;
		for (Integer vbnum : couchSplit.vbuckets) {
			vbids[i] = vbnum.shortValue();
			i++;
		}
		final String namebase = conf.get(CouchbaseConfig.CB_INPUT_STREAM_NAME,
				"hadoop");
		tapReq.setVbucketlist(vbids);
		final String streamName = namebase + "_"
				+ UUID.randomUUID().toString();
		tapReq.setName(streamName);

		tapclient = new TapClient(ClientURIList, bucket, password);
		try {
			tapclient.tapCustom(streamName, tapReq);
		} catch (ConfigurationException e) {
			throw new IOException(e);
		}
		
	}

}
