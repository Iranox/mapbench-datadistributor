package org.bsbmloader.helper;

import java.util.ArrayList;

public class ReviewHelper {
	private ArrayList<String[]> value;
	private ArrayList<String> product;
	private ArrayList<String> producer;
	private ArrayList<String> person;
	private ArrayList<String> builder;
	
	
	public ArrayList<String> getBuilder() {
		return builder;
	}

	public void setBuilder(ArrayList<String> builder) {
		this.builder = builder;
	}

	public ArrayList<String> getPerson() {
		return person;
	}

	public void setPerson(ArrayList<String> person) {
		this.person = person;
	}

	public String getProductTitle(String reviewID){
		return product.get(Integer.parseInt(reviewID)-1);
	}
	
	public String getProducerTitle(String reviewID){
		return producer.get(Integer.parseInt(reviewID)-1);
	}
	
	public String getPersonName(String reviewID){
		return person.get(Integer.parseInt(reviewID)-1);
	}
	
	public String getBuilderName(String reviewID){
		return builder.get(Integer.parseInt(reviewID)-1);
	}
	
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

	
	
	
	
	
	

}
