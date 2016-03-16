package org.aksw.es.bsbmloader.loader;

import java.util.HashMap;
import java.util.Map;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class SimpleTableUpdater extends Thread {
	private UpdateableDataContext dataContext;
	private String sourceTable;
	private String targetTable;
	private String forgeinKey;
	private String primaryKey;
	private int limit;
	private int offset;
	private static final String SUFFIX_NEW_TABLE = "_mat";

	public void setForgeinKey(String forgeinKey) {
		this.forgeinKey = forgeinKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public SimpleTableUpdater(String sourceTable, String targetTable, UpdateableDataContext dataContext) {
		this.sourceTable = sourceTable;
		this.targetTable = targetTable;
		this.dataContext = dataContext;
		limit = 1000;
		offset = 0;
	}

	public SimpleTableUpdater(UpdateableDataContext dataContext, String sourceTable, String targetTable, String forgeinKey,
			String primaryKey) {
		super();
		this.dataContext = dataContext;
		this.sourceTable = sourceTable;
		this.targetTable = targetTable;
		this.forgeinKey = forgeinKey;
		this.primaryKey = primaryKey;
	}

	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public Map<String, Object> getForgeinKeyValue(String pKey, Object value) {
		Map<String, Object> nestedObj = new HashMap<String, Object>();
		Table table = dataContext.getTableByQualifiedLabel(sourceTable);
		DataSet dataSet = dataContext.query().from(sourceTable).selectAll().where(pKey).isEquals(value).execute();
		while (dataSet.next()) {
			for (Column column : table.getColumns()) {
				nestedObj.put(column.getName(), dataSet.getRow().getValue(column));
			}
		}

		dataSet.close();

		return nestedObj;
	}

	public void insertData() {
		dataContext.executeUpdate(new UpdateScript() {
			public void run(UpdateCallback callback) {
				Table table = dataContext.getTableByQualifiedLabel(targetTable);
				DataSet dataSet = dataContext.query().from(targetTable.replace(SUFFIX_NEW_TABLE, "")).selectAll().limit(limit)
						.offset(offset).execute();
				RowInsertionBuilder rowsInsert = callback.insertInto(table);
				while (dataSet.next()) {
					Row row = dataSet.getRow();
					for (Column columnInsert : table.getColumns()) {
						if (!columnInsert.getName().equals(forgeinKey)) {
							rowsInsert.value(columnInsert.getName(), row.getValue(columnInsert.getColumnNumber()));
						} else {
							rowsInsert.value(columnInsert.getName(),
									getForgeinKeyValue(primaryKey, row.getValue(columnInsert.getColumnNumber())));
						}

					}
					rowsInsert.execute();
				}

			}

		});

	}

	public void run() {
		insertData();
	}

}
