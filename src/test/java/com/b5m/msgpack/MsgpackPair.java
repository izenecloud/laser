package com.b5m.msgpack;

import java.io.IOException;

import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;

public class MsgpackPair {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		MsgpackClient client = new MsgpackClient("localhost", 28611, "tare");
		Object[] req = new Object[0];
		Value res = client.asyncRead(req, "test");
		Converter converter = new org.msgpack.unpacker.Converter(res);
		pair vec = converter.read(pair.class);
		converter.close();
		System.out.println(vec.first);
		System.out.println(vec.second);
	}

}
