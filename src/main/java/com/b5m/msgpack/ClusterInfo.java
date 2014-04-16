package com.b5m.msgpack;

import java.util.Map;

import org.msgpack.annotation.Message;
@Message
public class ClusterInfo {
	public String clustername;
	public Long clusterDocNum;
	public Long clusterHash;
	public Map<Integer, Float> pows;
}
