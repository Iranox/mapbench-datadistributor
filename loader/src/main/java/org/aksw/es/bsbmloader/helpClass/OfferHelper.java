package org.aksw.es.bsbmloader.helpClass;

import java.util.ArrayList;

public class OfferHelper {
	private ArrayList<String[]> value;
	private ArrayList<String> product;
	private ArrayList<String> producer;
	private ArrayList<String> vendor;
	private ArrayList<String> builder;
	public ArrayList<String[]> getValue() {
		return value;
	}
	public void setValue(ArrayList<String[]> value) {
		this.value = value;
	}
	public ArrayList<String> getProduct() {
		return product;
	}
	public void setProduct(ArrayList<String> product) {
		this.product = product;
	}
	public ArrayList<String> getProducer() {
		return producer;
	}
	public void setProducer(ArrayList<String> producer) {
		this.producer = producer;
	}
	public ArrayList<String> getVendor() {
		return vendor;
	}
	public void setVendor(ArrayList<String> vendor) {
		this.vendor = vendor;
	}
	public ArrayList<String> getBuilder() {
		return builder;
	}
	public void setBuilder(ArrayList<String> builder) {
		this.builder = builder;
	}
	
	public String getProductTitle(String reviewID){
		return product.get(Integer.parseInt(reviewID)-1);
	}
	
	public String getProducerTitle(String reviewID){
		return producer.get(Integer.parseInt(reviewID)-1);
	}
	
	public String getVendorName(String reviewID){
		return vendor.get(Integer.parseInt(reviewID)-1);
	}
	
	public String getBuilderName(String reviewID){
		return builder.get(Integer.parseInt(reviewID)-1);
	}
	

}
