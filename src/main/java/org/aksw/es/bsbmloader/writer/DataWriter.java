package org.aksw.es.bsbmloader.writer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.aksw.es.bsbmloader.parser.ElementParser;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Table;

public class DataWriter implements Runnable {
	private BlockingQueue<Row> queue = null;
	private UpdateableDataContext dataContext;
	private Row row;
	private Table table;
	private CountDownLatch latch;

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
		
		row = queue.take();
		
		while (row.size() > 0) {
			row = queue.take();
			if (row.size() > 0) {
				dataContext.executeUpdate(insertScript());
			} else {
				return;
			}
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
						value = new ElementParser().getDate(row.getValue(column));
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
