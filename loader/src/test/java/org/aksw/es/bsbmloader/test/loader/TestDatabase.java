package org.aksw.es.bsbmloader.test.loader;

import org.aksw.es.bsbmloader.database.DatabaseBuilder;

import junit.framework.TestCase;

public class TestDatabase extends TestCase {
	public void testSetDatasource(){
		DatabaseBuilder data = new DatabaseBuilder();
		data.setConnectionProperties("jdbc:mysql://localhost/benchmark", "root", "password");
		assertNotNull(data.getDatasource());
		assertEquals("test", data.getDatasource().getUrl());
		assertEquals("root", data.getDatasource().getUsername());
		assertEquals("password", data.getDatasource().getPassword());
		
	}
	
	public void testSetConnection(){
		DatabaseBuilder data = new DatabaseBuilder();
		data.setConnectionProperties("jdbc:mysql://localhost/benchmark", "root", "password");
		data.setConnection(data.getDatasource());
		assertNotNull(data.getConnection());
	}
	

	

}
