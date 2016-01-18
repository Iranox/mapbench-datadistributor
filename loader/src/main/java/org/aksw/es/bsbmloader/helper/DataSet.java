package org.aksw.es.bsbmloader.helper;

import java.util.ArrayList;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;

public class DataSet {
	Row rows;
	Row[] fkRow;
	public Row getRows() {
		return rows;
	}
	public void setRows(Row rows) {
		this.rows = rows;
	}
	public Row[] getFkRow() {
		return fkRow;
	}
	public void setFkRow(Row[] fkRow) {
		this.fkRow = fkRow;
	}
	
	
	
}
