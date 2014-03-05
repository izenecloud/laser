package com.b5m.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {
	String trainResultPath;
	public String getTrainResultPath() {
		return trainResultPath;
	}
	public void setTrainResultPath(String trainResultPath) {
		this.trainResultPath = trainResultPath;
	}
	String type;
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public PropertiesLoader(String path){
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(path);   
		if(inputStream == null){
			System.out.println("Error properties file");
			System.exit(1);
		}
		Properties p = new Properties();   
		  try {   
			  p.load(inputStream);
			  trainResultPath = p.getProperty("trainResultPath");
			  type = p.getProperty("type");
		  } catch (IOException e1) {   
			  e1.printStackTrace(); 
			  System.exit(1);
		  }
	}
}
