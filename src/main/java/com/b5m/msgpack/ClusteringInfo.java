package com.b5m.msgpack;

import java.util.Map;

import org.msgpack.annotation.Message;
@Message
public class ClusteringInfo {
	public Integer clusteringIndex;
	public Map<Integer, Float> pows;
}
