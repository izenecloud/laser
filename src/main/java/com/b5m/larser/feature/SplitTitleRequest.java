package com.b5m.larser.feature;

import org.msgpack.annotation.Message;


@Message
public class SplitTitleRequest {

	private String tilte;

	public SplitTitleRequest() {
		
	}
	public SplitTitleRequest(String title) {
		this.tilte = title;
	}

}
