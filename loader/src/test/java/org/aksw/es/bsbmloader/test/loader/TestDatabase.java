package org.aksw.es.bsbmloader.test.loader;

import org.aksw.es.bsbmloader.loader.Database;

import junit.framework.TestCase;

public class TestDatabase extends TestCase {
	public void testSetDatasource(){
		Database data = new Database();
		data.setConnectionProperties("jdbc:mysql://localhost/benchmark", "root", "password");
		assertNotNull(data.getDatasource());
		assertEquals("test", data.getDatasource().getUrl());
		assertEquals("root", data.getDatasource().getUsername());
		assertEquals("password", data.getDatasource().getPassword());
		
	}
	
	public void testSetConnection(){
		Database data = new Database();
		data.setConnectionProperties("jdbc:mysql://localhost/benchmark", "root", "password");
		data.setConnection(data.getDatasource());
		assertNotNull(data.getConnection());
	}
	

	

}
