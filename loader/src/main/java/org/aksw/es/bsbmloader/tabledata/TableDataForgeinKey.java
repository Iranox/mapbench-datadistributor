package org.aksw.es.bsbmloader.tabledata;

import java.util.HashMap;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Table;

public class TableDataForgeinKey {
	private Table table;
	private HashMap<String, Row> rows = new HashMap<String, Row>();
	
	
	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}
	

	public HashMap<String, Row> getRows() {
		return rows;
	}

	public void setRows(HashMap<String, Row> rows) {
		this.rows = rows;
	}
	
	


}
