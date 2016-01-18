package org.aksw.es.bsbmloader.tabledata;

import java.util.ArrayList;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;

public class TableData {
	private Schema schema;
	private Column[] columns;
	private ArrayList<Row> rows = new ArrayList<Row>();
	private ArrayList<Column[]> fkColumns = new ArrayList<Column[]>();
	private ArrayList<Row[]> fkRows = new ArrayList<Row[]>();

	public ArrayList<Column[]> getFkColumns() {
		return fkColumns;
	}

	public void setFkColumns(ArrayList<Column[]> fkColumns) {
		this.fkColumns = fkColumns;
	}

	public ArrayList<Row[]> getFkRows() {
		return fkRows;
	}

	public void setFkRows(ArrayList<Row[]> fkRows) {
		this.fkRows = fkRows;
	}

	private ArrayList<DataSet> dataset = new ArrayList<DataSet>();

	public ArrayList<DataSet> getDataset() {
		return dataset;
	}

	public void setDataset(ArrayList<DataSet> dataset) {
		this.dataset = dataset;
	}

	private String table;

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public ArrayList<Row> getRows() {
		return rows;
	}

	public void setRows(ArrayList<Row> rows) {
		this.rows = rows;
	}

	public Column[] getColumns() {
		return columns;
	}

	public void setColumns(Column[] columns) {
		this.columns = columns;
	}

	public void setRow(Row dataset) {
		rows.add(dataset);
	}

}
