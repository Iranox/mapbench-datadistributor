package org.aksw.es.bsbmloader.writer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.Update;

public class DataSimpleUpdater implements Runnable {
	private BlockingQueue<Row> queue = null;
	private UpdateableDataContext dataContext;
	private Row row;
	private Table targetTable;
	private CountDownLatch latch;
	private Column forgeinKey;
	private String primaryKey;
	private String source;
	
	

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public DataSimpleUpdater(CountDownLatch latch, Table table, BlockingQueue<Row> queue, UpdateableDataContext dc) {
		this.queue = queue;
		this.targetTable = table;
		this.latch = latch;
		this.dataContext = dc;
	}

	public void setForgeinKey(Column key) {
		this.forgeinKey = key;
	}

	public void insertData() throws Exception {

		while ((row = queue.take()) != null) {
			if (row.size() > 0) {
				Object primary = row.getValue(dataContext.getTableByQualifiedLabel(source).getColumnByName(primaryKey));
				dataContext.executeUpdate(new Update(targetTable).where(forgeinKey).eq(primary).value(forgeinKey, createDataObject()));
			} else {
				return;
			}
		}
	}

	private Map<String, Object> createDataObject() {
		Map<String, Object> nestedObj = new HashMap<String, Object>();
		for (SelectItem column : row.getSelectItems()) {
			nestedObj.put(column.getColumn().getName(), row.getValue(column));
		}
		return nestedObj;
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
