package org.aksw.es.bsbmloader.bsbmloader;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.es.bsbmloader.reader.DataReader;
import org.aksw.es.bsbmloader.reader.TableReader;
import org.aksw.es.bsbmloader.writer.DataWriter;
import org.aksw.es.bsbmloader.writer.TableCreator;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Table;

public class NoSQLImport extends Import {

	private BlockingQueue<Row> queue;
	private TableCreator tableCreator = new TableCreator();
	private CountDownLatch latch;
	private ExecutorService executor;
	private String columnName;
	private int key;
	private int result;
	private final static int NUMBEROFTHREADS = 4;

	public NoSQLImport() {
		queue = new ArrayBlockingQueue<Row>(getBORDER());
		executor = Executors.newFixedThreadPool(NUMBEROFTHREADS);
		latch = new CountDownLatch(NUMBEROFTHREADS);
	}
	
	public void setHorzitalData(String columnName, int key, int result){
		this.key = key;
		this.result = result;
		this.columnName = columnName;
	}

	public void startImport() throws Exception {
		TableReader tableReader = new TableReader(getDatacontextSource());

		for (Table table : tableReader.getTables(getDatabaseName())) {
			createTablesinTargetDatabase(table);
			importToTarget(table);
		}
	}

	public void startImportVertikal(String tableNames[]) throws Exception {
		for (String tableName : tableNames) {
			Table sourceTable = getDatacontextSource().getTableByQualifiedLabel(tableName);
			createTablesinTargetDatabase(sourceTable);
			importToTarget(sourceTable);
		}

	}

	public void startHorizontalImport(String tableNames[]) throws Exception {
		for (String tableName : tableNames) {
			Table sourceTable = getDatacontextSource().getTableByQualifiedLabel(tableName);
			createTablesinTargetDatabase(sourceTable);
			importToTargetHori(sourceTable);
		}
	}

	private void createTablesinTargetDatabase(Table table) throws Exception {
		tableCreator.setDataContext(getFirstDatacontextTarget());
		tableCreator.createTable(table, null);
		// datacontextTarget = tableCreator.getDataContext();

	}

	public void importToTargetHori(Table table) throws Exception {
		
		executor.execute(createDataReaderHori(table, latch));
		executor.execute(createDataWriter(table, latch));
		executor.execute(createDataWriter(table, latch));
		executor.execute(createDataWriter(table, latch));

		latch.await();
		executor.shutdown();
	}

	public void importToTarget(Table table) throws Exception {
		executor = Executors.newFixedThreadPool(NUMBEROFTHREADS);
		latch = new CountDownLatch(NUMBEROFTHREADS);
		
		executor.execute(createDataReader(table, latch));
		executor.execute(createDataWriter(table, latch));
		executor.execute(createDataWriter(table, latch));
		executor.execute(createDataWriter(table, latch)); 

		latch.await();
		executor.shutdown();

	}

	private DataReader createDataReader(Table table, CountDownLatch latch) throws Exception {
		DataReader dataReader = new DataReader();
		dataReader.setDataContext(getDatacontextSource());
		dataReader.setQueue(queue);
		dataReader.setTable(table);
		dataReader.setLatch(latch);
		return dataReader;

	}

	private DataReader createDataReaderHori(Table table, CountDownLatch latch) throws Exception {
		DataReader dataReader = new DataReader();
		dataReader.setDataContext(getDatacontextSource());
		dataReader.setColumnName(columnName);
		dataReader.setHorizontal(true);
		dataReader.setKey(key);
		dataReader.setResult(result);
		dataReader.setQueue(queue);
		dataReader.setTable(table);
		dataReader.setLatch(latch);
		dataReader.setColumnName(columnName);
		return dataReader;

	}

	private DataWriter createDataWriter(Table table, CountDownLatch latch) throws Exception {
		DataWriter dataWriter = new DataWriter();
		dataWriter.setQueue(queue);
		dataWriter.setTable(table);
		dataWriter.setUpdateableDataContext(getFirstDatacontextTarget());
		dataWriter.setLatch(latch);
		return dataWriter;
	}

	public void createDataContext(JdbcDataContext datacontext) {
		setDataContext(datacontext);

	}

}
