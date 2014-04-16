package com.b5m.msgpack;

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
