package org.bsbmloader.helpClass;

public class ProductHelper {
	private String[] value;
	private String[] producer;
	private String[] producefeature;
	private String[] producerTags = {"nr", "label","comment","homepage","country","publisher","publishDate"};
	
	
	
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
	
	public String[] getProducefeature() {
		return producefeature;
	}
	
	public void setProducefeature(String[] producefeature) {
		this.producefeature = producefeature;
	}
	
	

}
