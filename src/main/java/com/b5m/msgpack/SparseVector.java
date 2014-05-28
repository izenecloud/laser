package com.b5m.msgpack;

import java.util.Map;

import org.msgpack.annotation.Message;

@Message
public class SplitTitleResponse {
	private Map<Integer, Float> termList;

	public Map<Integer, Float> getResponse() {
		return termList;
	}
}
