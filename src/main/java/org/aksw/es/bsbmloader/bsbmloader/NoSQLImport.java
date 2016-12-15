package org.aksw.es.bsbmloader.bsbmloader;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.aksw.es.bsbmloader.reader.DataReader;
import org.aksw.es.bsbmloader.reader.TableReader;
import org.aksw.es.bsbmloader.writer.DataWriter;
import org.aksw.es.bsbmloader.writer.TableCreator;
import org.apache.log4j.Logger;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.jdbc.dialects.MysqlQueryRewriter;
import org.apache.metamodel.schema.Table;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

public class NoSQLImport extends Import {

	private BlockingQueue<Row> queue;
	private TableCreator tableCreator;
	private CountDownLatch latch;
	private ExecutorService executor;
	private String columnName;
	private int key;
	private int result;
	private final static int NUMBEROFTHREADS = 4;
	static final MetricRegistry metrics = new MetricRegistry();
	private static org.apache.log4j.Logger log = Logger.getLogger(NoSQLImport.class);
	private String type;
	
	public NoSQLImport() {
		queue = new ArrayBlockingQueue<Row>(getBORDER());
	
		
		tableCreator = new TableCreator();
	}
	
	public void setType(String type){
		this.type = type;
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
		if(!type.equals("jdbc")){
			tableCreator.createTable(table, null);
		}else{
			tableCreator.createTable(table, new MysqlQueryRewriter((JdbcDataContext) getFirstDatacontextTarget()));
		}
	}

	public void importToTarget(Table table) throws Exception {
		latch = new CountDownLatch(NUMBEROFTHREADS);
		executor = Executors.newFixedThreadPool(NUMBEROFTHREADS);
		startReport();
		
		executor.execute(createDataReader(table, latch));
		executor.execute(createDataWriter(table, latch));
		executor.execute(createDataWriter(table, latch));
		executor.execute(createDataWriter(table, latch)); 

		latch.await();
		executor.shutdown();

	}

	public void importToTargetHori(Table table) throws Exception {
		startReport();
		executor = Executors.newFixedThreadPool(NUMBEROFTHREADS);
		latch = new CountDownLatch(NUMBEROFTHREADS);
		
		executor.execute(createDataReaderHori(table, latch));
		executor.execute(createDataWriter(table, latch));
		executor.execute(createDataWriter(table, latch));
		executor.execute(createDataWriter(table, latch));
	
		latch.await();
		executor.shutdown();
	}

	public void createDataContext(JdbcDataContext datacontext) {
		setDataContext(datacontext);
	
	}

	private DataReader createDataReader(Table table, CountDownLatch latch) throws Exception {
		DataReader dataReader = new DataReader(metrics);
		dataReader.setDataContext(getDatacontextSource());
		dataReader.setQueue(queue);
		dataReader.setTable(table);
		dataReader.setLatch(latch);
		return dataReader;

	}

	private DataReader createDataReaderHori(Table table, CountDownLatch latch) throws Exception {
		DataReader dataReader = new DataReader(metrics);
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
		DataWriter dataWriter = new DataWriter(metrics);
		dataWriter.setQueue(queue);
		dataWriter.setTable(table);
		dataWriter.setUpdateableDataContext(getFirstDatacontextTarget());
		dataWriter.setLatch(latch);
		return dataWriter;
	}

	private static void startReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .build();
        reporter.start(1, TimeUnit.MINUTES);
    }


}
