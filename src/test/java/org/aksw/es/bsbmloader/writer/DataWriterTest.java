package org.aksw.es.bsbmloader.writer;

import static org.junit.Assert.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.es.bsbmloader.embeddedH2Server.EmbeddedH2Server;
import org.aksw.es.bsbmloader.embeddedmongoserver.EmbeddedMongoServer;
import org.aksw.es.bsbmloader.reader.DataReader;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DB;

public class DataWriterTest {

	private DataWriter writer;
	private DataReader reader;
	private UpdateableDataContext source;
	private UpdateableDataContext target;
	private EmbeddedMongoServer test;
	private BlockingQueue<Row> queue;
	private final static int MAXIMALTHREADS = 2;
	private final static String TESTTABLENAME = "TESTTABLE";
	private final static String TESTDATABASENAME = "test";
/**
	@Before
	public void setup() throws Exception {
		queue = new ArrayBlockingQueue<Row>(100);
		test = new EmbeddedMongoServer();
		source = new JdbcDataContext(new EmbeddedH2Server().getClient());
		target = new MongoDbDataContext(new DB(test.getClient(), TESTDATABASENAME));
		createReader();
		createWriter();
		createTable();
	
	}

	@Test
	public void testDataWriter() throws Exception {
		reader.readData();
		writer.insertData();
		assertEquals(1, test.getClient().getDatabase(TESTDATABASENAME).getCollection(TESTTABLENAME).count());
	}
	
	@Test
	public void testDataWriterParrallel() throws Exception{
		ExecutorService executor = Executors.newFixedThreadPool(MAXIMALTHREADS);
		CountDownLatch latch = new CountDownLatch(MAXIMALTHREADS);
		
		writer.setLatch(latch);
		reader.setLatch(latch);
		
		executor.execute(reader);
		executor.execute(writer);
		
		latch.await();
		executor.shutdown();
		
		assertNotEquals(0, test.getClient().getDatabase(TESTDATABASENAME).getCollection(TESTTABLENAME).count());
		
	}
	
	public void createTable(){
		TableCreator creator = new TableCreator();
		creator.setDataContext(target);
		creator.createTable(source.getTableByQualifiedLabel(TESTTABLENAME), null);
	}
	
	private void  createReader() {
		reader = new DataReader();
		reader.setDataContext(source);
		reader.setTable(source.getTableByQualifiedLabel(TESTTABLENAME));
		reader.setQueue(queue);
		

	}

	private void createWriter() throws Exception {
		writer = new DataWriter();
		writer.setTable(source.getTableByQualifiedLabel(TESTTABLENAME));
		writer.setUpdateableDataContext(target);
		writer.setQueue(queue);
	} **/

}
