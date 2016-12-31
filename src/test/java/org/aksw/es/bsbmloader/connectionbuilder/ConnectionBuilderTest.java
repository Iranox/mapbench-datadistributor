package org.aksw.es.bsbmloader.connectionbuilder;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.aksw.es.bsbmloader.connectionproperties.JdbcConnectionProperties;
import org.aksw.es.bsbmloader.connectionproperties.MongoConnectionProperties;
import org.junit.Before;
import org.junit.Test;

public class ConnectionBuilderTest {

	private ConnectionBuilder builder = null;

	@Before
	public void setup() {
		builder = new ConnectionBuilder();
	}

	@Test
	public void testConnectionBuilderMySql() {
		ConnectionDatabase database = builder.createConnectionProperties("mysql");
		assertTrue(database instanceof JdbcConnectionProperties);
	}
	
	@Test
	public void testConnectionBuilderMongoDb() {
		ConnectionDatabase database = builder.createConnectionProperties("mongodb");
		assertTrue(database instanceof MongoConnectionProperties);
	}
	
	@Test
	public void testConnectionBuilderOther() {
		ConnectionDatabase database = builder.createConnectionProperties("otherDatabase");
		assertNull(database);
	}

}
