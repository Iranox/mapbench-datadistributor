package org.aksw.es.bsbmloader.nosqlloader;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

//TODO Still need ?
public class TableCopier extends Thread {
	private UpdateableDataContext dataContext;
	private String sourceTable;
	private String targetTable;
	private int limit;
	private int offset;
	
	public TableCopier(String sourceTable, String targetTable, UpdateableDataContext dataContext){
		this.sourceTable = sourceTable;
		this.targetTable = targetTable;
		this.dataContext = dataContext;
		limit = 1000;
		offset = 0;
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
	
	public void run() {
		dataContext.executeUpdate(new UpdateScript() {
			public void run(UpdateCallback callback) {
				Table table = dataContext.getTableByQualifiedLabel(targetTable);
				DataSet dataSet = dataContext.query().from(sourceTable).selectAll().limit(limit).offset(offset).execute();
				RowInsertionBuilder rowsInsert = callback.insertInto(table);
				while (dataSet.next()) {
					Row row = dataSet.getRow();
					for (Column columnInsert : table.getColumns()) {
						rowsInsert.value(columnInsert.getName(), row.getValue(columnInsert.getColumnNumber()));

					}
					rowsInsert.execute();
				}

			}

		});

	} 
}
