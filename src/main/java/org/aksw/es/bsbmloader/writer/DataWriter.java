package org.aksw.es.bsbmloader.writer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.aksw.es.bsbmloader.parser.ElementParser;
import org.aksw.es.bsbmloader.reader.PosionRow;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Table;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class DataWriter implements Runnable {
	private BlockingQueue<Row> queue = null;
	private UpdateableDataContext dataContext;
	private Row row;
	 private  final MetricRegistry metrics;
	private Table table;
	private CountDownLatch latch;
	
	public DataWriter( MetricRegistry metrics ){
		this.metrics = metrics;
		
	}

	public void setLatch(CountDownLatch latch) {
		this.latch = latch;
	}

	public void setQueue(BlockingQueue<Row> queue) {
		this.queue = queue;
	}

	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dataContext = dc;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public void insertData() throws Exception {
		Meter requests = metrics.meter("write threads" );
		dataContext.refreshSchemas();

		row = queue.take();

		while (!row.equals(PosionRow.posionRow)) {
			dataContext.executeUpdate(insertScript());
			requests.mark();
			row = queue.take();
		}
	}

	private UpdateScript insertScript() {
		UpdateScript insertScrript = new UpdateScript() {

			public void run(UpdateCallback callback) {
				Object value = null;
				RowInsertionBuilder insertData = callback.insertInto(table);
				for (SelectItem column : row.getSelectItems()) {
					if (!column.getColumn().getType().isTimeBased()) {
						value = row.getValue(column);
					} else {
						value = ElementParser.getDate(row.getValue(column));
					}
					insertData.value(column.getColumn(), value);
				}
				insertData.execute();

			}
		};

		return insertScrript;
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
