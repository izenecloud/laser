package com.b5m.larser.online;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.BooleanOptionHandler;
import org.kohsuke.args4j.spi.FloatOptionHandler;
import org.kohsuke.args4j.spi.IntOptionHandler;
import org.kohsuke.args4j.spi.StringOptionHandler;

import com.b5m.args4j.URIOptionHandler;

public class LaserOnlineModelTrainerArguments {
	  public static final Set<String> VALID_ARGUMENTS = new HashSet<String>(Arrays.asList(
	            "-outputPath",
	            "-signalPath",
	            "-regularizationFactor",
	            "-addIntercept",
	            "-columnsToExclude",
	            "-numFeatures"));

	    @Option(name = "-outputPath", required = true, handler = URIOptionHandler.class)
	    private URI outputPath;

	    @Option(name = "-signalPath", required = true, handler = StringOptionHandler.class)
	    private String signalPath;
	    
	    @Option(name = "-numFeatures", required = true, handler = IntOptionHandler.class)
	    private int numFeatures;

	    @Option(name = "-regularizationFactor", required = false, handler = FloatOptionHandler.class)
	    private float regularizationFactor;

	    @Option(name = "-addIntercept", required = false, handler = BooleanOptionHandler.class)
	    private boolean addIntercept;

	    @Option(name = "-columnsToExclude", required = false, handler = StringOptionHandler.class)
	    private String columnsToExclude;

	    public URI getOutputPath() {
	        return outputPath;
	    }

	    public String getSignalPath() {
	        return signalPath;
	    }

	    public float getRegularizationFactor() {
	        return regularizationFactor;
	    }

	    public boolean getAddIntercept() {
	        return addIntercept;
	    }

	    public String getColumnsToExclude() {
	        return columnsToExclude;
	    }
	    
	    public int getNumFeatures() {
	    	return numFeatures;
	    }
}
