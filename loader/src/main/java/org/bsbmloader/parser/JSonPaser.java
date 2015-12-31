package org.bsbmloader.parser;

import org.bsbmloader.helpClass.JSonObjectHelperClass;
import org.bsbmloader.helpClass.SimpleJSonHelpClass;
import org.json.JSONException;
import org.json.JSONObject;

public class JSonPaser {
	private JSONObject json = new JSONObject();
	public String simpleJSON(SimpleJSonHelpClass jsonData){
		
		String[] tags = jsonData.getTags();
		String[] value = jsonData.getValue();
		for(int index = 0; index < value.length; index++ ){
			try{
				json.put(tags[index], value[index]);
			}catch(JSONException e){
				e.printStackTrace();
			}	
		}
		return json.toString();
	}
	
	
//	TODO Refactor
	public String JSONwithObject(JSonObjectHelperClass jsonData){
		JSONObject json2 = new JSONObject();
		String[] tags = jsonData.getTags();
		String[] value = jsonData.getValue();
		String[] secondTags = getTagsFromObject(jsonData.getJsonObject());
		String[] secondValue = getValueFromObject(jsonData.getJsonObject());
		try{
			
			for(int index = 0; index < value.length; index++ ){
				json2.put(tags[index], value[index]);		
			}
			for(int index = 0; index < secondValue.length; index++ ){
				json.put(secondTags[index], secondValue[index]);		
			}
			
			json2.put(jsonData.getObjectTag(), json);
	  	
		}catch (JSONException e){
			e.printStackTrace();
		}
		return json2.toString();

	}
	
	private String[] getValueFromObject(SimpleJSonHelpClass jsonData){
		return jsonData.getValue();
	}
	
	private String[] getTagsFromObject(SimpleJSonHelpClass jsData){
		return jsData.getTags();
	}
	
	

}
