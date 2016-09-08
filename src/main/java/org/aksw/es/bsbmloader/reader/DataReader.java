package org.aksw.es.bsbmloader.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

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
	private CountDownLatch latch;
	private final static int BORDER = 50;
	private int numbers = 0;
	private boolean isFinish = false;

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
		queue.put(new PosionRow().getPosionRow());
		queue.put(new PosionRow().getPosionRow());
		queue.put(new PosionRow().getPosionRow());
	}

	private DataSet createDataSet() {
//		TODO Create function Count for this part
		if (numbers == 0 && !isFinish) {
			DataSet number = dataContext.query().from(table).selectCount().execute();
			number.next();
			Number n = (Number) number.getRow().getValue(0);
			numbers = n.intValue();
			limit = BORDER;
		}
//		Refactor ///////////////////////////////////////////////////////


		if (numbers > BORDER && !isFinish) {
			this.limit = BORDER;
		}

		if (limit == 0 && !isFinish) {
			return dataContext.query().from(table).selectAll().execute();
		}

		return dataContext.query().from(table).selectAll().offset(offset).limit(limit).execute();

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
		if (numbers < BORDER) {
			numbers = 0;
			isFinish = true;
		} 
	}

	private void insertRowIntoBlockingQueue(DataSet dataset) throws InterruptedException {
		while (dataset.next()) {
			Row row = dataset.getRow();
			row.getSelectItems();
			queue.put(dataset.getRow());
		}

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
