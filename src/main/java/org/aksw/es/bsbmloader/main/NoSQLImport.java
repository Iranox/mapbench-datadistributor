package org.aksw.es.bsbmloader.main;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.es.bsbmloader.connectionbuilder.ConnectionCreator;
import org.aksw.es.bsbmloader.reader.DataReader;
import org.aksw.es.bsbmloader.reader.TableReader;
import org.aksw.es.bsbmloader.writer.DataWriter;
import org.aksw.es.bsbmloader.writer.TableCreator;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.jdbc.dialects.MysqlQueryRewriter;
import org.apache.metamodel.jdbc.dialects.PostgresqlQueryRewriter;
import org.apache.metamodel.schema.Table;

public class NoSQLImport {
	private UpdateableDataContext datacontextSource = null;
	private UpdateableDataContext datacontextTarget = null;
	private final int BORDER = 1000;
	private String databaseName;
	private BlockingQueue<Row> queue;
	private CountDownLatch latch;

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void createDataContext(String url,String user, String password) throws Exception {
			datacontextSource = new ConnectionCreator().createConnection(url, user, password, null);
	}

	public void createDataContextTarget(String url,String user, String password, String type) throws Exception {
		datacontextTarget = new ConnectionCreator().createConnection(url, user, password, databaseName, type);
	}

	public void startImport() throws Exception {
		TableReader tableReader = new TableReader(datacontextSource);
		for (Table table : tableReader.getTables(databaseName)) {
			createTargetTables(table);
			importToTarget(table);
		}
	}

	// TODO rename
	public void createTargetTables(Table table) throws Exception {
		TableCreator tableCreator = new TableCreator();
		tableCreator.setDataContext(datacontextTarget);
		tableCreator.createTable(table, null);
		datacontextTarget = tableCreator.getDataContext();

	}

	public void importToTarget(Table table) throws Exception {
		queue = new ArrayBlockingQueue<Row>(BORDER);
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
		dataReader.setDataContext(datacontextSource);
		dataReader.setQueue(queue);
		dataReader.setTable(table);
		dataReader.setLatch(latch);
		return dataReader;

	}

	private DataWriter createDataWriter(Table table, CountDownLatch latch) throws Exception {
		DataWriter dataWriter = new DataWriter();
		dataWriter.setQueue(queue);
		dataWriter.setTable(table);
		dataWriter.setUpdateableDataContext(datacontextTarget);
		dataWriter.setLatch(latch);
		return dataWriter;
	}

	private IQueryRewriter getIQueryRewriter(String url) {

		if (url.contains("mysql")) {
			return new MysqlQueryRewriter(null);
		}
		if (url.contains("postgesql")) {
			return new PostgresqlQueryRewriter(null);
		}

		return null;
	}

}
