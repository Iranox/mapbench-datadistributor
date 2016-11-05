package org.aksw.es.bsbmloader.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Table;

public class DataReaderHorizontal implements Runnable {

	private DataContext source;
	private BlockingQueue<Row>[] queue = null;
	private int limit = 200;
	private int acutelDataSetNumber = 0;
	private int totalDataSetNumber = 0;
	private int offset = 0;
	private Table table;
	private CountDownLatch latch;
	
	private final static int OFFSET_NUMBER = 200;

	private static org.apache.log4j.Logger log = Logger.getLogger(DataReaderHorizontal.class);

	public DataReaderHorizontal(DataContext source, Table table, BlockingQueue<Row>... queue) {
		this.source = source;
		this.queue = queue;
		this.table = table;
	}

	public void readData() throws Exception {
		countDataSets();
		while(totalDataSetNumber > 0){
			getData();
			totalDataSetNumber -= OFFSET_NUMBER;
		}
	
	}
	
	// TODO rename function
	private void getData() throws Exception {
		DataSet ds = source.query().from(table).selectAll().limit(limit).offset(offset).execute();
		while (ds.next()) {
			queue[chooseDatacontext()].put(ds.getRow());
			acutelDataSetNumber++;
		}
		offset += OFFSET_NUMBER;
		ds.close();
	}

	private void countDataSets() {
		DataSet ds = source.query().from(table).selectCount().execute();
		ds.next();
		totalDataSetNumber = (Integer) ds.getRow().getValue(0);
		ds.close();
	}
	
	private int chooseDatacontext(){
		return acutelDataSetNumber % queue.length;
	}
	
	public void setLatch(CountDownLatch latch) {
		this.latch = latch;
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
