package org.aksw.es.bsbmloader.tabledata;

import org.apache.metamodel.data.Row;

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
