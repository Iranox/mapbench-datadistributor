package org.aksw.es.bsbmloader.parser;

import java.sql.Timestamp;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ElementParserTest {
	
	private Date utilDate;
	
	@Before
	public void setup() {
		utilDate = new Date();
	}
	
	@Test
	public void testCreateClass(){
		ElementParser ele = new ElementParser();
		assertNotNull(ele);
	}
	
	@Test
	public void testParseTimeSqlDate(){
		java.sql.Date sqlDate = new java.sql.Date( utilDate.getTime());
		Object timeObject = sqlDate;
		assertEquals(utilDate,  ElementParser.getDate(timeObject));
	}
	
	@Test
	public void testParseTimeTimeStamp(){
		Timestamp time = new Timestamp(utilDate.getTime());
		Object timeStampObject = time;
		assertEquals(utilDate,  ElementParser.getDate(timeStampObject));
	}
}
