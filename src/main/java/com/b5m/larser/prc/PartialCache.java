package com.b5m.larser.prc;

import java.util.concurrent.TimeoutException;

import com.b5m.larser.dispatch.Response;

/*
 * PRC:
 * Partial Results Cache
 */
public class PartialCache {
	public Response get(String key, Long expire) throws TimeoutException{
		return null;
	}
	
	public void put(String key, Object value) {
	}
}
