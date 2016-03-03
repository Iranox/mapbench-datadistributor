package org.aksw.es.bsbmloader.loader;


import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.CreateTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class ElasticLoader {
private UpdateableDataContext dataContext;
	
	public void setUpdateableDataContext(UpdateableDataContext dataContext) throws Exception {
		this.dataContext = dataContext;
	}
	
	public void createTable(Table table, Column[] column) {
		final CreateTable createTable = new CreateTable(dataContext.getDefaultSchema(),table.getName());
		for(Column columnTable : column){
			if(columnTable.getName() == "nr"){
				createTable.withColumn(columnTable.getName()).ofType(columnTable.getType()).asPrimaryKey();
			}else{
				createTable.withColumn(columnTable.getName()).ofType(columnTable.getType());
			}
			
			
		}
		
		dataContext.executeUpdate(createTable);
	}

}
