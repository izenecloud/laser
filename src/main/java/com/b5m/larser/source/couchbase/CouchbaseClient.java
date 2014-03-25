package com.b5m.larser.source.couchbase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

public class CouchbaseClient {
	com.couchbase.client.CouchbaseClient client = null;

	public CouchbaseClient() throws IOException {
		ArrayList<URI> nodes = new ArrayList<URI>();

		// Add one or more nodes of your cluster (exchange the IP with yours)
		nodes.add(URI.create(CouchbaseConfig.CB_INPUT_CLUSTER));
		client = new com.couchbase.client.CouchbaseClient(nodes, CouchbaseConfig.CB_INPUT_BUCKET, CouchbaseConfig.CB_INPUT_STREAM_NAME, CouchbaseConfig.CB_INPUT_PASSWORD);
	}
}
