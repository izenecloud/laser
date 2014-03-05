package com.b5m.larser.dispatch;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public abstract class Component {
	private static Path CONPONENT_BASE;
	
	public abstract void run() throws IOException;
	
	public static void setComponentBasePath(Path base) {
		CONPONENT_BASE = base;
	}
	
	public Path getComponentOutputPath(String name) {
		return new Path(CONPONENT_BASE, name);
	}
}
