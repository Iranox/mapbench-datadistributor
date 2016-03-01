package org.aksw.es.bsbmloader.loader;


import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.CreateTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class ElasticLoader {
private UpdateableDataContext dc;
	
	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dc = dc;
	}
	
	public void createTable(Table table, Column[] column) {
		System.out.println(dc);
		final CreateTable createTable = new CreateTable(dc.getDefaultSchema(),table.getName());
		for(Column columnTable : column){
			if(columnTable.getName() == "nr"){
				createTable.withColumn(columnTable.getName()).ofType(columnTable.getType()).asPrimaryKey();
			}else{
				createTable.withColumn(columnTable.getName()).ofType(columnTable.getType());
			}
			
			
		}
		
		dc.executeUpdate(createTable);
	}

}
