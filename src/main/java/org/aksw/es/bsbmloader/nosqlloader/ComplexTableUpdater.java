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

//TODO Scale Test for this class
public class ComplexTableUpdater extends Thread{

	private UpdateableDataContext dataContext;
	private Table joinTable;
	private String secondSource;
	private String pkSecond;
	private int offset;
	private int limit;
	private String secondFkey;
	private String forgeinKey;
	private String joinTableName;
	
	
	public ComplexTableUpdater(){
		this.offset = 0;
		this.limit = 0;
	}
	
	public void setSecondSource(String secondSource) {
		this.secondSource = secondSource;
	}

	public void setPkSecond(String pkSecond) {
		this.pkSecond = pkSecond;
	}

	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}
	

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	public void setProperty(String joinTable, String secondFkey, String forgeinKey){
		this.joinTableName = joinTable;
		this.secondFkey = secondFkey;
		this.forgeinKey = forgeinKey;
	}

	public void compressData(String joinTable, String secondFkey, String forgeinKey) {
		this.joinTable = dataContext.getTableByQualifiedLabel(joinTable);
		Column primaryColumn = dataContext.getTableByQualifiedLabel(joinTable).getColumnByName(secondFkey);
		DataSet dataSet = dataContext.query().from(joinTable).select(primaryColumn).groupBy(primaryColumn).limit(limit).offset(offset).execute();
		while (dataSet.next()) {
			Object dataValue = dataSet.getRow().getValue(primaryColumn); 
			DataSet dataSetArray = dataContext.query().from(joinTable).select(forgeinKey).where(primaryColumn)
					.eq(dataValue).execute();
			insertNewFormat(dataValue, datasetToArray(dataSetArray), primaryColumn,forgeinKey);
			dataSetArray.close();
		}

		dataSet.close();
	}
	
	@Override
	public void run(){
		compressData(joinTableName, secondFkey, forgeinKey);
		
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
