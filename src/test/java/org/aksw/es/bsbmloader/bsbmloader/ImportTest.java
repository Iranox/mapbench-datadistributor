package org.aksw.es.bsbmloader.bsbmloader;

import static org.junit.Assert.assertEquals;

import org.aksw.es.bsbmloader.importer.Import;
import org.junit.Before;
import org.junit.Test;

public class ImportTest {
	
	private Import importer; 
	final private String TESTSTRING = "test";
	
	@Before
	public void setup(){
		importer = new Import();
	}
	
	@Test
	public void testSetPrimary(){
		importer.setPrimary(TESTSTRING);
		assertEquals(TESTSTRING,importer.getPrimary());
	}
	
	@Test
	public void testDatabaseName(){
		importer.setDatabaseName(TESTSTRING);;
		assertEquals(TESTSTRING,importer.getDatabaseName());
	}
}
