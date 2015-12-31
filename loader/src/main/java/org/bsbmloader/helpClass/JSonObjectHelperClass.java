package org.bsbmloader.helpClass;

public class JSonObjectHelperClass extends SimpleJSonHelpClass {
	
	private SimpleJSonHelpClass jsonObject;
	private String objectTag;

	public JSonObjectHelperClass(String[] tags, String[] value) {
		super(tags, value);
	}

	public String getObjectTag() {
		return objectTag;
	}

	public void setObjectTag(String objectTag) {
		this.objectTag = objectTag;
	}

	public JSonObjectHelperClass(String[] tags, String[] value, SimpleJSonHelpClass jsonObject) {
		super(tags, value);
		this.jsonObject = jsonObject;
	}

	public SimpleJSonHelpClass getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(SimpleJSonHelpClass jsonObject) {
		this.jsonObject = jsonObject;
	}
	
	
	

}
