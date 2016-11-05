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

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Table;

public class NoSQLImport extends Import {

	private UpdateableDataContext[] dataContexts = null;
	private BlockingQueue<Row> queue;
	private CountDownLatch latch;
	
	public void createTargetDatacontexts (String url[],String user[], String password[], String type[]){
		
	}

	public void startImport() throws Exception {
		TableReader tableReader = new TableReader(getDatacontextSource());
		
		for (Table table : tableReader.getTables(getDatabaseName())) {
			createTablesinTargetDatabase(table);
			importToTarget(table);
		}
	}
	
	public void startImportVertikal (String tableNames[]) throws Exception {
		for(String tableName : tableNames){
			Table sourceTable = getDatacontextSource().getTableByQualifiedLabel(tableName);
			createTablesinTargetDatabase(sourceTable);
			importToTarget(sourceTable);
		}
		
	}
	
	public void createTablesinTargetDatabase(Table table) throws Exception {
		TableCreator tableCreator = new TableCreator();
		tableCreator.setDataContext(getFirstDatacontextTarget());
		tableCreator.createTable(table, null);
//		datacontextTarget = tableCreator.getDataContext();

	}

	public void importToTarget(Table table) throws Exception {
		queue = new ArrayBlockingQueue<Row>(getBORDER());
		ExecutorService executor = Executors.newFixedThreadPool(4);
		latch = new CountDownLatch(4);

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
