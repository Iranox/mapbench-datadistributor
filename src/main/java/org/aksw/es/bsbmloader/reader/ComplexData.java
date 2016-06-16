package org.aksw.es.bsbmloader.reader;

import java.util.ArrayList;

import org.apache.metamodel.data.Row;

public class ComplexData {
	private Row primaryValue;
	private String primaryKey;
	private ArrayList<Row> arrayData;

	public ComplexData(Row primaryValue, ArrayList<Row> arrayData) {
		super();
		this.primaryValue = primaryValue;
		this.arrayData = arrayData;
	}

	public Row getPrimaryValue() {
		return primaryValue;
	}

	public void setPrimaryValue(Row primaryValue) {
		this.primaryValue = primaryValue;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public ArrayList<Row> getArrayData() {
		return arrayData;
	}

	public void setArrayData(ArrayList<Row>  arrayData) {
		this.arrayData = arrayData;
	}

}
