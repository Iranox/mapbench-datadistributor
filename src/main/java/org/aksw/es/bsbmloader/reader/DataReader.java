package org.aksw.es.bsbmloader.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class DataReader implements Runnable {
	private DataContext dataContext;
	private BlockingQueue<Row> queue = null;
	private Table table;
	private int offset = 0;
	private int limit = 0;
	private static org.apache.log4j.Logger log = Logger.getLogger(DataReader.class);
	private CountDownLatch latch;
	private final static int BORDER = 300;
	private int numbers = 0;
	private boolean isFinish = false;
	private int key = 0;
	private int result = 0;
	private boolean horizontal = false;
	private String columnName;
	private Column hashedColumn;
	private  final MetricRegistry metrics;
	private Meter requests;
	
	public DataReader(MetricRegistry metrics){
		this.metrics = metrics;
		
	}

	public void setKey(int key) {
		this.key = key;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public void setHorizontal(boolean horizontal) {
		this.horizontal = horizontal;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public void setLatch(CountDownLatch latch) {
		this.latch = latch;
	}

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
		queue.put(PosionRow.posionRow);
		queue.put(PosionRow.posionRow);
		queue.put(PosionRow.posionRow);
	}

	private Query selectAll() {
		return dataContext.query().from(table).selectAll().offset(offset).limit(limit).toQuery();
	}

	private DataSet createDataSet() {
		if (numbers == 0 && !isFinish) {
			DataSet number = dataContext.query().from(table).selectCount().execute();
			number.next();
			Number n = (Number) number.getRow().getValue(0);
			numbers = n.intValue();
			limit = BORDER;
			number.close();
		}

		if (limit == 0 && !isFinish) {
			return dataContext.query().from(table).selectAll().execute();
		}

		return dataContext.executeQuery(selectAll());

	}

	public void readData() throws Exception {

		table = dataContext.getTableByQualifiedLabel(table.getName());
		DataSet dataSet = createDataSet();

		while (numbers > 0) {
			insertRowIntoBlockingQueue(dataSet);
			dataSet.close();
			setNewOffset();
			dataSet = createDataSet();
		}

		insertPosion();
	}

	private void setNewOffset() {
		offset += BORDER;
		numbers -= BORDER;
		if (numbers < BORDER && numbers <= 0) {
			numbers = 0;
			isFinish = true;
		}
	}

	private void insertRowIntoBlockingQueue(DataSet dataset) throws InterruptedException {
		
		 hashedColumn = table.getColumnByName(columnName);
		 requests = metrics.meter("read thread");
		 

		if (!horizontal) {
			
		while (dataset.next()) {
			
				Row row = dataset.getRow();
				row.getSelectItems();
				queue.put(row);
				requests.mark();
			}
		} else {
			while (dataset.next()) {
				Row row = dataset.getRow();
				row.getSelectItems();
				if (isKey(row)) {
					queue.put(row);
					requests.mark();
				}

			}
		}
	}

	private boolean isKey(Row row) {
		return ( (Integer) row.getValue(hashedColumn) % key ) == result;
	}
	

	public void run() {
		try {
			readData();
			latch.countDown();
		} catch (Exception e) {

			log.error(e);
		}

	}

}
