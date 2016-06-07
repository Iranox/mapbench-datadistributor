package org.aksw.es.bsbmloader.main;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.es.bsbmloader.connectionbuilder.ConnectionCreator;
import org.aksw.es.bsbmloader.reader.DataReader;
import org.aksw.es.bsbmloader.reader.TableReader;
import org.aksw.es.bsbmloader.writer.DataWriter;
import org.aksw.es.bsbmloader.writer.TableCreator;
import org.apache.commons.cli.CommandLine;
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
	private BlockingQueue<Row> queue ;
	
	

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void createDataContext(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("urlMysql")) {
			datacontextSource = new ConnectionCreator().createConnection(commandLine, "mysql");
		}
		datacontextTarget = new ConnectionCreator().createConnection(commandLine, null);
	}
	
// TODO rename
	public void createTargetTables(CommandLine commandLine) throws Exception {
		TableReader tableReader = new TableReader();
		tableReader.setDataContext(datacontextSource);
		Table[] tables = tableReader.getTables(databaseName);
		TableCreator tableCreator = new TableCreator();
		tableCreator.setUpdateableDataContext(datacontextTarget);
		if (!commandLine.hasOption("targetUrl")) {
			tableCreator.createTable(tables, null);
		} else {
			tableCreator.createTable(tables, getIQueryRewriter(commandLine.getOptionValue("targetUrl")));
		}

	}
	
	public void importToTarget() throws Exception{
		queue = new ArrayBlockingQueue<Row>(BORDER);
		for(Table table : datacontextSource.getSchemaByName(databaseName).getTables()){
			ExecutorService executor = Executors.newCachedThreadPool();
			executor.execute(createDataReader(table));
			executor.execute(createDataWriter(table));
			executor.execute(createDataWriter(table));
			executor.execute(createDataWriter(table));
			executor.shutdown();
		}
		
	}
	
	private DataReader createDataReader(Table table) throws Exception {
		DataReader dataReader = new DataReader();
		dataReader.setDataContext(datacontextSource);
		dataReader.setQueue(queue);
		dataReader.setTable(table);
		return dataReader;
	
	}
	
	private DataWriter createDataWriter(Table table) throws Exception{
		DataWriter dataWriter = new DataWriter();
		dataWriter.setQueue(queue);
		dataWriter.setUpdateableDataContext(datacontextTarget);
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
