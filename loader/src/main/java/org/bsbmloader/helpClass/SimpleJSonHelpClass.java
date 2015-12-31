package org.bsbmloader.helpClass;

import java.util.Arrays;

import org.bsbmloader.parser.JSonPaser;

public class SimpleJSonHelpClass {
	private String[] tags;
	private String[] value;
	
	public SimpleJSonHelpClass(String[] tags, String[] value){
		this.tags = tags;
		this.value = value;
	}
	
	public String[] getTags(){
		return tags;
	}
	
	public String[] getValue(){
		return value;
	}

	@Override
	public String toString() {
		
		return new JSonPaser().simpleJSON(new SimpleJSonHelpClass(tags, value));
	}
	
	
	


}
