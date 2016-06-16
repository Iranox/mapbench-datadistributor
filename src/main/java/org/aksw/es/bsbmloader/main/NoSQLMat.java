package org.aksw.es.bsbmloader.main;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.es.bsbmloader.connectionbuilder.ConnectionCreator;
import org.aksw.es.bsbmloader.reader.DataReader;
import org.aksw.es.bsbmloader.writer.DataSimpleUpdater;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class NoSQLMat {
	
	private UpdateableDataContext datacontextSource = null;
	private UpdateableDataContext datacontextTarget = null;
	private UpdateableDataContext datacontextTarget1 = null;
	private UpdateableDataContext datacontextTarget2 = null;
	private String targetKey;
	private final int BORDER = 1000;
	private String databaseName;
	private BlockingQueue<Row> queue;
	private CountDownLatch latch;
	private Table target;
	private String primary;
	
	

	public void setPrimary(String primary) {
		this.primary = primary;
	}

	public void setForgeinKey(String primaryKey) {
		this.targetKey = primaryKey;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void createDataContext(String url,String user, String password, String type) throws Exception {
		datacontextSource = new ConnectionCreator().createConnection(url, user, password, databaseName, type);
	}

	public void createDataContextTarget(String url,String user, String password, String type) throws Exception {
		datacontextTarget = new ConnectionCreator().createConnection(url, user, password, databaseName, type);
		datacontextTarget1 =new ConnectionCreator().createConnection(url, user, password, databaseName, type);
	    datacontextTarget2 = new ConnectionCreator().createConnection(url, user, password, databaseName, type);
	}
	
	public void setTargetTable(String tableName){
		 target = datacontextTarget.getTableByQualifiedLabel(tableName);
	}

	public void importToTarget(String table) throws Exception {
		queue = new ArrayBlockingQueue<Row>(BORDER);
		ExecutorService executor = Executors.newFixedThreadPool(4);
		latch = new CountDownLatch(4);
		executor.execute(createDataReader(datacontextSource.getTableByQualifiedLabel(table), latch));
		executor.execute(createDataWriter(target, latch,datacontextSource.getTableByQualifiedLabel(table),datacontextTarget));
		executor.execute(createDataWriter(target, latch,datacontextSource.getTableByQualifiedLabel(table),datacontextTarget1));
		executor.execute(createDataWriter(target, latch,datacontextSource.getTableByQualifiedLabel(table),datacontextTarget2));

		latch.await();
		executor.shutdown();

	}

	private DataReader createDataReader(Table table, CountDownLatch latch) throws Exception {
		DataReader dataReader = new DataReader();
		dataReader.setDataContext(datacontextSource);
		dataReader.setQueue(queue);
		dataReader.setTable(table);
		dataReader.setLatch(latch);
		return dataReader;

	}

	private DataSimpleUpdater createDataWriter(Table table, CountDownLatch latch, Table source, UpdateableDataContext dc) throws Exception {
		DataSimpleUpdater dataWriter = new DataSimpleUpdater(latch, table, queue, dc);
		Column clumn = datacontextTarget.getTableByQualifiedLabel(table.getName()).getColumnByName(targetKey);
		dataWriter.setForgeinKey(clumn);
		dataWriter.setSource(source.getName());
		dataWriter.setPrimaryKey(primary);
	
		return dataWriter;
	}



}
