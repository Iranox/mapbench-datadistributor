package org.aksw.es.bsbmloader.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.aksw.es.bsbmloader.reader.ComplexData;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class DataComplexUpdater implements Runnable {
	private BlockingQueue<ComplexData> queue = null;
	private UpdateableDataContext dataContext;
	private Table targetTable;
	private CountDownLatch latch;
	ComplexData complexDataObject;
	private String forgeinKey;
	private Column primaryKey;
	final static ComplexData POSION = null;

	public DataComplexUpdater(CountDownLatch latch, Table table, BlockingQueue<ComplexData> queue,
			UpdateableDataContext dc) {
		this.queue = queue;
		this.targetTable = table;
		this.latch = latch;
		this.dataContext = dc;
	}
	
	

	public void setPrimrayKey(String key) {
		this.primaryKey = targetTable.getColumnByName(key);
	}
	
	public void setForgeinKey(String forgeinkey){
		this.forgeinKey = forgeinkey;
	}

	public void insertData() throws Exception {
		while (((complexDataObject = queue.take())) == POSION) {
			if (complexDataObject.getPrimaryValue().size() > 0) {
				dataContext.executeUpdate(insertComplexData());
			} else {
				return;
			}
		}
		dataContext.executeUpdate(insertComplexData());

	}

	private UpdateScript insertComplexData() {
		UpdateScript update = null;
		update = new UpdateScript() {

			public void run(UpdateCallback callback) {
				RowInsertionBuilder insertRow = callback.insertInto(targetTable);
				for (SelectItem column : complexDataObject.getPrimaryValue().getSelectItems()) {
					insertRow.value(column.getColumn().getName(), complexDataObject.getPrimaryValue().getValue(column));
				}
				System.out.println(complexDataObject.getArrayData().get(0));
				insertRow.value(forgeinKey, createDataObject(complexDataObject.getArrayData()));

				System.out.println(insertRow.toSql());
			}
		};
		return update;
	}

	private ArrayList<Map<String, Object>> createDataObject(ArrayList<Row> rowArray) {
		ArrayList<Map<String, Object>> nestedObjArray = new ArrayList<Map<String, Object>>();
		System.out.println(rowArray.size());
		for (Row row : rowArray) {
			Map<String, Object> nestedObj = new HashMap<String, Object>();
			for (SelectItem column : row.getSelectItems()) {
				nestedObj.put(column.getColumn().getName(), row.getValue(column));
			}
			nestedObjArray.add(nestedObj);
		}

		return nestedObjArray;
	}

	public void run() {
		try {
			insertData();
			latch.countDown();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
