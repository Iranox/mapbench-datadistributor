package org.aksw.es.bsbmloader.writer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.aksw.es.bsbmloader.parser.ElementParser;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Table;

public class DataWriter implements Callable<Integer> {
	private BlockingQueue<Row> queue = null;
	private UpdateableDataContext dataContext;
	private Row row;
	private Table table;

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

		while ((row = queue.take()) != null) {
			if (row.size() > 0) {
				dataContext.executeUpdate(insertScript());
			}else{
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
		  
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Integer call() throws Exception {
		
			try {
				insertData();
			  
			} catch (Exception e) {
				e.printStackTrace();
			}
		return 0;
	}



}
