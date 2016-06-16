package org.aksw.es.bsbmloader.reader;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;


import org.apache.log4j.Logger;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;

public class ComplexDataReader {
	private DataContext dataContext;
	private BlockingQueue<ComplexData> queueComplexData = null;
	private static org.apache.log4j.Logger log = Logger.getLogger(ComplexDataReader.class);
	private CountDownLatch latch;
	final static ComplexData POSIONROW = null;
	private String secondFkey;
	private String joinTable;
	private String forgeinKey;
	private String pkSecond;
	private String secondSource;
	ArrayList<Row> arrayData;
	
	

	public ComplexDataReader(DataContext dataContext, BlockingQueue<ComplexData> queueComplexData,
			CountDownLatch latch) {
		super();
		this.dataContext = dataContext;
		this.queueComplexData = queueComplexData;
		this.latch = latch;
		arrayData = new ArrayList<Row>();
	}



	public void setLatch(CountDownLatch latch) {
		this.latch = latch;
	}



	public void setDataContext(DataContext dataContext) {
		this.dataContext = dataContext;
	}



	private void insertPosion() throws Exception {
		queueComplexData.put(POSIONROW);
		queueComplexData.put(POSIONROW);
		queueComplexData.put(POSIONROW);
	}
	

	
	
	public void readComplexData() throws Exception {
		Column primaryColumn = dataContext.getTableByQualifiedLabel(joinTable).getColumnByName(secondFkey);
		DataSet dataSet = dataContext.query().from(joinTable).select(primaryColumn).groupBy(primaryColumn).execute();
		while (dataSet.next()) {
			Object dataValue = dataSet.getRow().getValue(primaryColumn);
			DataSet dataSetArray = dataContext.query().from(joinTable).select(forgeinKey).where(primaryColumn)
					.eq(dataValue).execute();
			a(dataSetArray);
			ComplexData complexDataObject = new ComplexData(dataSet.getRow(),arrayData);
			queueComplexData.put(complexDataObject);
			dataSetArray.close();
			
		}
		insertPosion();
		dataSet.close();

	}
	
	private void a(DataSet dataSetArry){
		while(dataSetArry.next()){
			getComplexData(dataSetArry.getRow().getValue(0));
		}
	}
	
	private void getComplexData(Object object){
		
		DataSet dataSet = dataContext.query().from(secondSource).selectAll().where(pkSecond).eq(object).execute();
		while(dataSet.next()){
			arrayData.add(dataSet.getRow());
		}
		dataSet.close();
	
	}


	public void run() {
		try {
			readComplexData();
			latch.countDown();
		} catch (Exception e) {

			log.error(e);
		}

	}
}
