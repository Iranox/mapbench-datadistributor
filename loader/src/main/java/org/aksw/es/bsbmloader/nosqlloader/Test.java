package org.aksw.es.bsbmloader.nosqlloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.delete.DeleteFrom;
import org.apache.metamodel.insert.InsertInto;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

//TODO Rename Class
//TODO Scale Test for this class
//TODO Implement Runnable or Extends Thread
public class Test {

	private UpdateableDataContext dataContext;
	private Table joinTable;
	String secondSource;
	String pkSecond;
	
	public void setSecondSource(String secondSource) {
		this.secondSource = secondSource;
	}

	public void setPkSecond(String pkSecond) {
		this.pkSecond = pkSecond;
	}

	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}

	public void compressData(String joinTable, String secondFkey, String forgeinKey) {
		this.joinTable = dataContext.getTableByQualifiedLabel(joinTable);
		Column primaryColumn = dataContext.getTableByQualifiedLabel(joinTable).getColumnByName(secondFkey);
		DataSet dataSet = dataContext.query().from(joinTable).select(primaryColumn).groupBy(primaryColumn).execute();
		while (dataSet.next()) {
			Object product = dataSet.getRow().getValue(primaryColumn); //TODO Rename
			DataSet dataSetArray = dataContext.query().from(joinTable).select(forgeinKey).where(primaryColumn)
					.eq(product).execute();
			insertNewFormat(product, datasetToArray(dataSetArray), primaryColumn,forgeinKey);
			dataSetArray.close();
		}

		dataSet.close();
	}

	private ArrayList<Map<String, Object>> datasetToArray(DataSet dataSetArray) {
		ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		while (dataSetArray.next()) {
			getComplexData(dataSetArray.getRow().getValue(0));
			list.add(getComplexData(dataSetArray.getRow().getValue(0)));
		}

		return list;
	}
	
	private Map<String, Object> getComplexData(Object object){
		Map<String, Object> nestedObj = new HashMap<String, Object>();
		DataSet dataSet = dataContext.query().from(this.secondSource).selectAll().where(this.pkSecond).eq(object).execute();
		while(dataSet.next()){
			for(SelectItem item :dataSet.getSelectItems()){
				nestedObj.put(item.getColumn().getName(), dataSet.getRow().getValue(item));
			}
		}
		dataSet.close();
		return nestedObj;
	}

	private void insertNewFormat(Object secondFk,ArrayList<Map<String, Object>> productFeature, Column fkColumn, String forgeinKey) {
		deleteOldRow(secondFk, fkColumn);
		dataContext.executeUpdate(
				new InsertInto(joinTable).value(fkColumn, secondFk).value(forgeinKey, productFeature.toArray()));
	}

	private void deleteOldRow(Object product, Column fkColumn) {
		dataContext.executeUpdate(new DeleteFrom(joinTable).where(fkColumn).eq(product));
	}
}
