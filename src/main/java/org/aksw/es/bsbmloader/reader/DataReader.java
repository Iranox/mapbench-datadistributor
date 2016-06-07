package org.aksw.es.bsbmloader.reader;

import java.util.concurrent.BlockingQueue;

import org.aksw.es.bsbmloader.posionrow.PosionRow;
import org.apache.log4j.Logger;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Table;

public class DataReader implements Runnable {
	private DataContext dataContext;
	private BlockingQueue<Row> queue = null;
	private Table table;
	private int offset = 0;
	private int limit = 0;
	private static org.apache.log4j.Logger log = Logger.getLogger(DataReader.class);

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public void setDataContext(DataContext dataContext) {
		this.dataContext = dataContext;
	}

	public void setQueue(BlockingQueue<Row> queue) {
		this.queue = queue;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	private void insertPosion() throws Exception {
		queue.add(new PosionRow().getPosion());
		queue.add(new PosionRow().getPosion());
		queue.add(new PosionRow().getPosion());
	}

	private DataSet createDataSet() {
		if (limit == 0) {
			return dataContext.query().from(table).selectAll().execute();
		}
		
		return dataContext.query().from(table).selectAll().offset(offset).limit(limit).execute();

	}

	public void readData() throws Exception {
		table = dataContext.getTableByQualifiedLabel(table.getName());
		DataSet dataSet = createDataSet();
		while (dataSet.next()) {
			queue.add(dataSet.getRow());
		}
		dataSet.close();
		insertPosion();
	}

	public void run() {
		try {
			readData();
		} catch (Exception e) {

			log.error(e);
		}

	}

}
