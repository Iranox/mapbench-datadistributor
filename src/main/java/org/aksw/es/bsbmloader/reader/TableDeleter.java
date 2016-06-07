package org.aksw.es.bsbmloader.reader;


import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.schema.Table;


//TODO To Small ?
public class TableDeleter {
	private UpdateableDataContext dataContext;
	
	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}
	
	
	public void deleteDatabase(Table table){
		if(dataContext.getTableByQualifiedLabel(table.getName())!= null){
			dataContext.executeUpdate(new DropTable(table.getName()));
		}
	}
	
}
