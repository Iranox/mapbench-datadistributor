package org.aksw.es.bsbmloader.bsbmloader;

import static org.junit.Assert.assertEquals;


import org.aksw.es.bsbmloader.embeddedH2Server.EmbeddedH2Server;
import org.aksw.es.bsbmloader.embeddedmongoserver.EmbeddedMongoServer;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;

public class NoSQLImportTest {

	private EmbeddedH2Server h2;
	private EmbeddedMongoServer mongo;
	private NoSQLImport noSQLImport;
	private MongoClient client;
	private final static String DATABASE = "PUBLIC";
	private final static String TEST_TABLE = "HELLOWORLD";

	@Before
	public void setup() throws Exception {
		h2 = new EmbeddedH2Server();
		mongo = new EmbeddedMongoServer();
		noSQLImport = new NoSQLImport();
		
		// Create a second table which contains a small data set 
		h2.createSecondTestTable();
		h2.insertSecondTestData();
		
		//Config
		JdbcDataContext datacontext = new JdbcDataContext(h2.getClient());
		client = mongo.getClient();
		noSQLImport.setDatabaseName(DATABASE);
		noSQLImport.createDataContext(datacontext);
		noSQLImport.createDataContextTarget(client.getAddress().toString(), "", "", "mongodb");
		
	}
	
	@After
	public void teardown(){
		h2.shutdown();
		mongo.shutdown();
	}

	@Test
	public void testNoSQLImport() throws Exception {
		String[] tableNames = { TEST_TABLE };
		noSQLImport.startImportVertikal(tableNames);
		assertEquals(1, client.getDatabase(DATABASE).getCollection(TEST_TABLE).count());
	}
	
	@Test
	public void testNoSQLImportAllTable() throws Exception {
		noSQLImport.startImport();
		assertEquals(1, client.getDatabase(DATABASE).getCollection(TEST_TABLE).count());
		assertEquals(1, client.getDatabase(DATABASE).getCollection("TESTTABLE").count());
	}

}
