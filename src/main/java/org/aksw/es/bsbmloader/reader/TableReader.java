package org.aksw.es.bsbmloader.reader;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

//TODO Needed ?
public class TableReader {
	private UpdateableDataContext dataContext;
	
	
	
	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}


	public Table[] getTables(String database) throws Exception{
		Schema schema = dataContext.getSchemaByName(database);
		Table[] tables = schema.getTables();
		return tables;
	}

}
