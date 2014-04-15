package com.b5m.msgpack;

import java.util.Map;

import org.msgpack.annotation.Message;

@Message
public class ClusterInfo {
	public String clustername;
	public Map<Integer, Float> pows;
	public Integer clusterDocNum;
	public Integer clusterHash;
}
