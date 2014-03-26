package com.b5m.lr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class ListWritable implements Writable {
	private Class<? extends Writable> valueClass;
	private Class<? extends List> listClass;
	private List<Writable> values;

	public ListWritable() {
	}

	public ListWritable(List<Writable> values) {
		listClass = values.getClass();
		valueClass = values.get(0).getClass();
		this.values = values;
	}

	public Class<? extends Writable> getValueClass() {
		return valueClass;
	}

	@SuppressWarnings("rawtypes")
	public Class<? extends List> getListClass() {
		return listClass;
	}

	public void set(List<Writable> values) {
		this.values = values;
	}

	public List<Writable> get() {
		return values;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void readFields(DataInput in) throws IOException {
		String listClass = in.readUTF();
		try {
			this.listClass = (Class<? extends List>) Class.forName(listClass);
			String valueClass = in.readUTF();
			this.valueClass = (Class<? extends Writable>) Class
					.forName(valueClass);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}

		int size = in.readInt(); // construct values
		try {
			values = this.listClass.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < size; i++) {
			Writable value = WritableFactories.newInstance(this.valueClass);
			value.readFields(in); // read a value
			values.add(value); // store it in values
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(listClass.getName());
		out.writeUTF(valueClass.getName());
		out.writeInt(values.size()); // write values
		Iterator<Writable> iterator = values.iterator();
		while (iterator.hasNext()) {
			iterator.next().write(out);
		}
	}

}
