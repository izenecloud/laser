package io.izenecloud.couchbase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringOptionHandler;

public class LaserArgument {
	public static final Set<String> VALID_ARGUMENTS = new HashSet<String>(
			Arrays.asList("-configure"));
	
	@Option(name = "-configure", required = true, handler = StringOptionHandler.class)
	private String configure;
	
	public String getConfigure() {
		return configure;
	}
}
