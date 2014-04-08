package com.b5m.larser.feature;

import org.msgpack.annotation.MessagePackBeans;

@MessagePackBeans
public class SplitTitleRequest {

	private final String tilte;

	public SplitTitleRequest(String title) {
		this.tilte = title;
	}

}
