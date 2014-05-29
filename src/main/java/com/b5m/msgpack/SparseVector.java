package com.b5m.msgpack;

import java.util.Map;

import org.msgpack.annotation.Message;

@Message
public class SparseVector {
	public Integer clustering;
	public Map<Integer, Float> vec;
}
