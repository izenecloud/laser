package com.b5m.msgpack;

import java.util.Iterator;
import java.util.List;

import org.msgpack.annotation.Ignore;
import org.msgpack.annotation.Message;

@Message
public class SparseVector {
	public List<Integer> index;
	public List<Float> value;
	
	@Ignore
	private Iterator<Integer> iit = null;
	@Ignore
	private Iterator<Float> vit = null;
	
	public boolean hasNext() {
		if (null == iit || null == vit) {
			iit = index.iterator();
			vit = value.iterator();
		}
		return iit.hasNext() && vit.hasNext();
	}
	
	public Integer getIndex() {
		return iit.next();
	}
	
	public Float get() {
		return vit.next();
	}
}
