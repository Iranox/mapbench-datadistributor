package org.aksw.es.bsbmloader.nosqlloader;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Table;

public class TableCounter {
	
	public int getRowNumber(UpdateableDataContext dataContext, String sourceTable) throws Exception{
		Number numbers = null;
		Table table = dataContext.getTableByQualifiedLabel(sourceTable);
		DataSet dataSet = dataContext.query().from(table).selectCount().execute();
		if(dataSet.next()){
			Row row = dataSet.getRow();
			numbers = (Number) row.getValue(0);
		}
		dataSet.close();
		return numbers.intValue();
		
	}

}
