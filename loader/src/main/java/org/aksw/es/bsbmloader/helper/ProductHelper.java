package org.aksw.es.bsbmloader.helper;

import java.util.ArrayList;

public class ProductHelper {
	private String[] value;
	private String[] producer;
	private ArrayList<String[]> producefeature;
	private String[] producerTags = {"nr", "label","comment","homepage","country","publisher","publishDate"};
	private ArrayList<String[]> producetype;
	private String[] vendor;
	
	
	
	public String[] getVendor() {
		return vendor;
	}

	public void setVendor(String[] vendor) {
		this.vendor = vendor;
	}

	public ArrayList<String[]> getProducetype() {
		return producetype;
	}

	public void setProducetype(ArrayList<String[]> producetype) {
		this.producetype = producetype;
	}

	public String[] getProducerTags() {
		return producerTags;
	}

	public void setProducerTags(String[] producerTags) {
		this.producerTags = producerTags;
	}

	public String[] getValue() {
		return value;
	}
	
	public void setValue(String[] value) {
		this.value = value;
	}
	
	public String[] getProducer() {
		return producer;
	}
	
	public void setProducer(String[] producer) {
		this.producer = producer;
	}

	public ArrayList<String[]> getProducefeature() {
		return producefeature;
	}

	public void setProducefeature(ArrayList<String[]> producefeature) {
		this.producefeature = producefeature;
	}
	
	public String[] getProducefeatureArray(){
		String[] data = new String[producefeature.size()];
		for(int i = 0; i < data.length;i++){
			data[i] = producefeature.get(i)[0];
		}
		return data;
	}
	
	public String[] getProducetypeArray(){
		String[] data = new String[producetype.size()];
		for(int i = 0; i < data.length;i++){
			data[i] = producetype.get(i)[0];
		}
		return data;
	}
	
	

	
	

}
