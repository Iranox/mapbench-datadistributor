package org.aksw.es.bsbmloader.tabledata;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Table;

public class TableDataPrimary {
	private Table table;
	private ArrayList<Row> rows = new ArrayList<Row>(); // alle Daten
	private HashMap<String, TableDataForgeinKey> fkTable1 = new HashMap<String, TableDataForgeinKey>(); // ForgeinTableName map Row und andere Daten wie Colums
	private HashMap<String, String> relationship = new HashMap<String, String>(); // ForgeinKey map ForgeinTableName
	//TODO  möglich SpaltenName auf ForgeinTable zu mappen

	public HashMap<String, TableDataForgeinKey> getFkTable1() {
		return fkTable1;
	}
	
	public TableDataForgeinKey getFKTable(String key){
		return fkTable1.get(key);
	}
	
	public String getRelationshipTable(String key){
		if(relationship.get(key) != null){
			return relationship.get(key);
		}
		return null;
	}

	public void setFkTable1(HashMap<String, TableDataForgeinKey> fkTable1) {
		this.fkTable1 = fkTable1;
	}

	public HashMap<String, String> getRelationship() {
		return relationship;
	}

	public void setRelationship(HashMap<String, String> relationship) {
		this.relationship = relationship;
	}

	public void insertFkTable(String key, TableDataForgeinKey value) {
		fkTable1.put(key, value);
	}

	public void insertRelationship(String key, String value) {
		relationship.put(key, value);
	}


	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public ArrayList<Row> getRows() {
		return rows;
	}

	public void setRows(ArrayList<Row> rows) {
		this.rows = rows;
	}

}
