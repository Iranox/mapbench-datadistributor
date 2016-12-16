package org.aksw.es.bsbmloader.importer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.es.bsbmloader.reader.DataReader;
import org.aksw.es.bsbmloader.writer.DataSimpleUpdater;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

import com.codahale.metrics.MetricRegistry;

public class NoSQLMat extends Import{
	
	private BlockingQueue<Row> queue;
	private CountDownLatch latch;
	static final MetricRegistry metrics = new MetricRegistry();
	

	public void importToTarget(String table) throws Exception {
		queue = new ArrayBlockingQueue<Row>(getBORDER());
		ExecutorService executor = Executors.newFixedThreadPool(getThreadsNumber()+1);
		latch = new CountDownLatch(getThreadsNumber()+1);
		
		executor.execute(createDataReader(getDatacontextSource().getTableByQualifiedLabel(table), latch));
		for(UpdateableDataContext target : getTargetDataContext()){
			executor.execute(createDataWriter(getTarget(), latch, getDatacontextSource().getTableByQualifiedLabel(table),target));
		}
		
		latch.await();
		executor.shutdown();

	}

	private DataReader createDataReader(Table table, CountDownLatch latch) throws Exception {
		DataReader dataReader = new DataReader(metrics);
		dataReader.setDataContext(getDatacontextSource());
		dataReader.setQueue(queue);
		dataReader.setTable(table);
		dataReader.setLatch(latch);
		return dataReader;

	}

	private DataSimpleUpdater createDataWriter(Table table, CountDownLatch latch, Table source, UpdateableDataContext dc) throws Exception {
		DataSimpleUpdater dataWriter = new DataSimpleUpdater(latch, table, queue, dc);
		Column clumn = getFirstDatacontextTarget().getTableByQualifiedLabel(table.getName()).getColumnByName(getTargetKey());
		dataWriter.setForgeinKey(clumn);
		dataWriter.setSource(source.getName());
		dataWriter.setPrimaryKey(getPrimary());
	
		return dataWriter;
	}



}
