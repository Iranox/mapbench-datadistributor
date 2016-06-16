package org.aksw.es.bsbmloader.reader;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class TableReader {
	private UpdateableDataContext dataContext;
	
	
	public TableReader( UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}
	
	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}
	
	public Column getColumn(String database, String primaryKey, String tableName){
		Schema schema = dataContext.getSchemaByName(database);
		Table table = schema.getTableByName(tableName);
		Column column = table.getColumnByName(primaryKey);
		return column;
	}


	public Table[] getTables(String database) throws Exception{
		System.out.println(database);
		Schema schema = dataContext.getSchemaByName(database);
		Table[] tables = schema.getTables();
		return tables;
	}

}
