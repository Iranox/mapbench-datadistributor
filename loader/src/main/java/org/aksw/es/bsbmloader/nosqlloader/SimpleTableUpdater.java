package org.aksw.es.bsbmloader.nosqlloader;


import java.util.HashMap;
import java.util.Map;


import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.Update;


;
public class SimpleTableUpdater extends Thread {
	private UpdateableDataContext dataContext;
	private String  source;
	private int limit;
	private int offset;
	private Column forgeinColumn;
	private Column primaryColumn;
	private Column[] sourceColumns;
	private Table targetTable;

	public SimpleTableUpdater(UpdateableDataContext dataContext, String source) {
		super();
		this.dataContext = dataContext;
		this.source = source;
		limit = 1000;
		offset = 0;
	}
	

	public void setLimit(int limit) {
		this.limit = limit;
	}


	public void setOffset(int offset) {
		this.offset = offset;
	}

	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}

	public void setSource(String source) {
		this.source = source;
	}

	
	public void setConnection(Column forgeinColumn, Column primaryColumn, Table targetTable, Column[] sourceColumns){
		 this.forgeinColumn = forgeinColumn;
		 this.primaryColumn = primaryColumn ;
		 this.sourceColumns = sourceColumns;
		 this.targetTable = targetTable;
	}
	
	public void updateData(){
		Map<String, Object> nestedObj = new HashMap<String, Object>();
		DataSet dataSet = dataContext.query().from(source).selectAll().limit(limit).offset(offset).execute();
		while (dataSet.next()) {
			Object primaryKeyObject = dataSet.getRow().getValue(primaryColumn);
			for (Column columns : sourceColumns) {
				if (dataSet.getRow().getValue(columns) != null) {
					nestedObj.put(columns.getName(), dataSet.getRow().getValue(columns));
				}
			}
			Update update = new Update(this.targetTable).where(forgeinColumn).eq(primaryKeyObject).value(forgeinColumn, nestedObj);
			dataContext.executeUpdate(update);
			
		}
		dataSet.close();
	}
	
	

	public void run(){
		updateData();
	}

}
