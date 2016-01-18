package org.aksw.es.bsbmloader.metamodell;

import java.util.ArrayList;

import org.aksw.es.bsbmloader.helper.Helper;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;


/**
 * @author Tobias
 */
public class NoSQLLoader{
	private UpdateableDataContext dc;
	
	public void insertSimpleTable(UpdateableDataContext da, ArrayList<Helper> data){
		dc = da;
		dc.executeUpdate(new UpdateScript() {
			private ArrayList<Helper> data;
			private Schema defaultSchema;

			public void run(UpdateCallback callback) {
			  for(int index = 0; index < data.size(); index++){
					TableCreationBuilder tableCreation = callback.createTable(defaultSchema, data.get(index).getTable());

				for(Column column : data.get(index).getColumns()){
				
					tableCreation.withColumn(column.getName().replace("nr", "_id"));
				}
				Table table = tableCreation.execute();
				
				RowInsertionBuilder rows = callback.insertInto(table);
				for(Row row: data.get(index).getRows()){
					for(Column column : data.get(index).getColumns()){
					   if(row.getValue(column.getColumnNumber())== null)
						   rows.value(table.getColumn(column.getColumnNumber()), "null");
					   else
						   rows.value(table.getColumn(column.getColumnNumber()), row.getValue(column.getColumnNumber()).toString());
	    
					}
					rows.execute();
				}
				
			  }
			}
			private UpdateScript init(ArrayList<Helper> data, Schema defaultSchema){
				this.data = data;
				this.defaultSchema = defaultSchema;
				return this;
			}
		}.init(data,dc.getDefaultSchema())
		);
	}


}
