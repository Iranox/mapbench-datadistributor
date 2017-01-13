package org.aksw.es.bsbmloader.writer;

import java.util.ArrayList;
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
	private ArrayList<Row> row = new ArrayList<Row>();
	 private  final MetricRegistry metrics;
	private Table table;
	private CountDownLatch latch;
	private Meter requests;
	private final int BORDER = 250;
	
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
		

		row.add(queue.take());
		requests = metrics.meter("write threads" );

		while (!row.contains((PosionRow.posionRow))) {
			
			
			
			if(row.size() == BORDER){
				dataContext.executeUpdate(insertScript());
				row.clear();
			
			}
			row.add(queue.take());
		}
		
		row.remove(PosionRow.posionRow);
		if(!row.isEmpty()){
			dataContext.executeUpdate(insertScript());
		}
		
	}

	private UpdateScript insertScript() {
		UpdateScript insertScrript = new UpdateScript() {

			public void run(UpdateCallback callback) {
				Object value = null;
				RowInsertionBuilder insertData = callback.insertInto(table);
				for(Row rows: row){
					for (SelectItem column : rows.getSelectItems()) {
						if (!column.getColumn().getType().isTimeBased()) {
							value = rows.getValue(column);
						} else {
							value = ElementParser.getDate(rows.getValue(column));
						}
						insertData.value(column.getColumn(), value);
					}
					requests.mark();
					insertData.execute();
				}
		
				

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
